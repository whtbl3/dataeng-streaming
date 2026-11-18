"""Coupled delivery-day simulation: the order lifecycle and the vans' GPS tell the same story.

The whole local day is precomputed from a seed (routes, arrivals, outcomes, promised
windows), then exposed as time-ordered order events and GPS pings. Pure logic, no I/O.

- Two waves per van, departing 08:00 and 14:00 Asia/Ho_Chi_Minh; orders are created in
  the hours before their wave and assigned 30 minutes before departure.
- Vans drive straight lines at 20-40 km/h (10% of hops are traffic jams), dwell 2-6 min
  at each stop, return to the depot, and park there (speed 0) for the rest of the day.
- Exactly `round(fail_rate * n)` orders fail and `round(late_rate * delivered)` delivered
  orders close after their promised window (the window is set from the true arrival).
"""

from __future__ import annotations

import heapq
import math
import random
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from simulator.fleet import HCMC_BBOX, KM_PER_DEG_LAT, MAX_SPEED_KMH, Ping
from simulator.world import DEPOT_BY_ID, VAN_CAPACITY, VANS, Depot

LOCAL_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
DAY_S = 86_400
# (departure, creation window start hour, creation window end hour) in local time
WAVES = ((time(8, 0), 0, 7), (time(14, 0), 7, 13))
ASSIGN_LEAD_S = 30 * 60
RADIUS_KM = 6.0
SPEED_KMH = (20.0, 40.0)
JAM_PROBABILITY = 0.10
JAM_FACTOR = (0.2, 0.4)
DWELL_S = (120, 360)
JITTER_M = 7.0  # per axis -> at most ~10 m
WINDOW_S = 2 * 3600
KIND_RANK = {"created": 0, "assigned": 1, "out_for_delivery": 2, "delivered": 3, "failed": 3}


@dataclass(frozen=True)
class OrderEvent:
    ts: datetime  # UTC
    kind: str  # created | assigned | out_for_delivery | delivered | failed
    order_id: str
    fields: dict = field(default_factory=dict)


@dataclass(frozen=True)
class OrderTruth:
    order_id: str
    depot_id: str
    van_id: str
    stop_seq: int
    dest_lat: float
    dest_lon: float
    created_at: datetime
    assigned_at: datetime
    out_at: datetime
    arrived_at: datetime
    closed_at: datetime
    outcome: str  # delivered | failed
    promised_start: datetime
    promised_end: datetime


@dataclass(frozen=True)
class _Segment:
    """Van is at (lat0,lon0) at t0 and at (lat1,lon1) at t1 (epoch seconds)."""

    t0: int
    t1: int
    lat0: float
    lon0: float
    lat1: float
    lon1: float
    speed_kmh: float  # 0 = parked / dwelling
    heading_deg: float


def _utc(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, UTC)


def _km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def _bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return math.degrees(math.atan2(y, x)) % 360.0


def _splitmix64(x: int) -> int:
    x = (x + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    return x ^ (x >> 31)


def _unit(x: int) -> float:
    """Deterministic float in [-1, 1) from an integer key."""
    return (_splitmix64(x) >> 11) / float(1 << 52) - 1.0


def _random_stop(rng: random.Random, depot: Depot) -> tuple[float, float]:
    angle = rng.uniform(0, 2 * math.pi)
    r_km = RADIUS_KM * math.sqrt(rng.random())
    lat = depot.lat + r_km * math.cos(angle) / KM_PER_DEG_LAT
    lon = depot.lon + r_km * math.sin(angle) / (KM_PER_DEG_LAT * math.cos(math.radians(depot.lat)))
    margin = 0.001
    lat = min(max(lat, HCMC_BBOX.lat_min + margin), HCMC_BBOX.lat_max - margin)
    lon = min(max(lon, HCMC_BBOX.lon_min + margin), HCMC_BBOX.lon_max - margin)
    return round(lat, 6), round(lon, 6)


def _nearest_neighbour(start: tuple[float, float], stops: list[tuple[float, float]]):
    route, pos, left = [], start, list(stops)
    while left:
        nxt = min(left, key=lambda s: _km(pos[0], pos[1], s[0], s[1]))
        left.remove(nxt)
        route.append(nxt)
        pos = nxt
    return route


@dataclass
class DayPlan:
    seed: int
    local_date: date
    interval_s: int
    day_start: int  # epoch seconds of local midnight
    van_ids: list[str]
    orders: dict[str, OrderTruth]
    events: list[OrderEvent]
    segments: dict[str, list[_Segment]]

    @property
    def start(self) -> datetime:
        return _utc(self.day_start)

    @property
    def end(self) -> datetime:
        """Exclusive end of the local day."""
        return _utc(self.day_start + DAY_S)

    def tick_at_or_before(self, ts: datetime) -> datetime:
        offset = int(ts.timestamp()) - self.day_start
        return _utc(self.day_start + (offset // self.interval_s) * self.interval_s)

    def pings(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        vehicle_prefix: str | None = None,
    ) -> Iterator[Ping]:
        """GPS pings on ticks in [start, end] (inclusive), time-major, vans in id order."""
        lo = self.day_start if start is None else max(int(start.timestamp()), self.day_start)
        hi = self.day_start + DAY_S - 1 if end is None else int(end.timestamp())
        hi = min(hi, self.day_start + DAY_S - 1)
        first = (
            self.day_start + math.ceil((lo - self.day_start) / self.interval_s) * self.interval_s
        )
        pointers = dict.fromkeys(self.van_ids, 0)
        names = {
            v: (v if vehicle_prefix is None else f"{vehicle_prefix}-{v.split('-')[-1]}")
            for v in self.van_ids
        }
        for t in range(first, hi + 1, self.interval_s):
            ts = _utc(t)
            for idx, van_id in enumerate(self.van_ids):
                segs = self.segments[van_id]
                i = pointers[van_id]
                while t > segs[i].t1 and i < len(segs) - 1:  # boundary tick -> earlier segment
                    i += 1
                pointers[van_id] = i
                yield self._ping(names[van_id], idx, t, ts, segs[i])

    def _ping(self, name: str, van_idx: int, t: int, ts: datetime, s: _Segment) -> Ping:
        frac = 0.0 if s.t1 == s.t0 else (t - s.t0) / (s.t1 - s.t0)
        lat = s.lat0 + (s.lat1 - s.lat0) * frac
        lon = s.lon0 + (s.lon1 - s.lon0) * frac
        key = (self.seed * 1_000_003 + van_idx) * 10_000_000_000 + t
        lat += _unit(key * 3 + 1) * JITTER_M / 1000 / KM_PER_DEG_LAT
        lon += _unit(key * 3 + 2) * JITTER_M / 1000 / (KM_PER_DEG_LAT * math.cos(math.radians(lat)))
        lat = min(max(round(lat, 6), HCMC_BBOX.lat_min), HCMC_BBOX.lat_max)
        lon = min(max(round(lon, 6), HCMC_BBOX.lon_min), HCMC_BBOX.lon_max)
        if s.speed_kmh == 0:
            speed, heading = 0.0, 0.0
        else:
            speed = min(max(s.speed_kmh + 2 * _unit(key * 3 + 3), 1.0), MAX_SPEED_KMH)
            speed, heading = round(speed, 1), round(s.heading_deg, 1) % 360.0
        return Ping(name, ts, lat, lon, speed, heading)


def simulate_day(
    *,
    seed: int,
    local_date: date,
    vans: int = len(VANS),
    orders_per_day: int = 5000,
    late_rate: float = 0.10,
    fail_rate: float = 0.02,
    prefix: str = "ORD",
    interval_s: int = 5,
) -> DayPlan:
    if not 1 <= vans <= len(VANS):
        raise ValueError(f"vans must be between 1 and {len(VANS)}")
    if orders_per_day < 0 or orders_per_day > vans * VAN_CAPACITY * len(WAVES):
        raise ValueError("orders_per_day exceeds fleet capacity")
    if not (0 <= late_rate <= 1 and 0 <= fail_rate <= 1) or interval_s < 1:
        raise ValueError("rates must be in [0, 1] and interval_s >= 1")

    rng = random.Random(f"{seed}|{local_date.isoformat()}")
    day_start = int(datetime.combine(local_date, time(0), LOCAL_TZ).timestamp())

    def local(t: time) -> int:
        return int(datetime.combine(local_date, t, LOCAL_TZ).timestamp())

    fleet = VANS[:vans]
    base, extra = divmod(orders_per_day, vans)
    pending: list[dict] = []
    segments: dict[str, list[_Segment]] = {}
    seq = 0

    for v_idx, van in enumerate(fleet):
        depot = DEPOT_BY_ID[van.depot_id]
        home = (depot.lat, depot.lon)
        k = base + (1 if v_idx < extra else 0)
        per_wave = (k - k // 2, k // 2)
        segs: list[_Segment] = []
        last_end = day_start
        for (dep_time, h0, h1), n_stops in zip(WAVES, per_wave, strict=True):
            if n_stops == 0:
                continue
            dep = max(local(dep_time), last_end)
            segs.append(_Segment(last_end, dep, *home, *home, 0.0, 0.0))
            route = _nearest_neighbour(home, [_random_stop(rng, depot) for _ in range(n_stops)])
            cur, pos = dep, home
            for stop_seq, stop in enumerate(route, start=1):
                dist = _km(*pos, *stop)
                speed = rng.uniform(*SPEED_KMH)
                if rng.random() < JAM_PROBABILITY:
                    speed *= rng.uniform(*JAM_FACTOR)
                travel = max(1, round(dist / speed * 3600))
                segs.append(
                    _Segment(
                        cur,
                        cur + travel,
                        *pos,
                        *stop,
                        dist / (travel / 3600),
                        _bearing(*pos, *stop),
                    )
                )
                arrival = cur + travel
                closed = arrival + rng.randint(*DWELL_S)
                segs.append(_Segment(arrival, closed, *stop, *stop, 0.0, 0.0))
                seq += 1
                created = rng.randint(local(time(h0)), local(time(h1)) - 1)
                pending.append(
                    {
                        "order_id": f"{prefix}-{local_date:%Y%m%d}-{seq:05d}",
                        "depot_id": depot.depot_id,
                        "van_id": van.van_id,
                        "stop_seq": stop_seq,
                        "dest": stop,
                        "created": created,
                        "assigned": dep - ASSIGN_LEAD_S,
                        "out": dep,
                        "arrival": arrival,
                        "closed": closed,
                    }
                )
                cur, pos = closed, stop
            back = _km(*pos, *home)
            speed = rng.uniform(*SPEED_KMH)
            travel = max(1, round(back / speed * 3600))
            segs.append(
                _Segment(
                    cur, cur + travel, *pos, *home, back / (travel / 3600), _bearing(*pos, *home)
                )
            )
            last_end = cur + travel
        if last_end >= day_start + DAY_S:
            raise ValueError(f"{van.van_id} cannot finish its routes within the day")
        segs.append(_Segment(last_end, day_start + DAY_S, *home, *home, 0.0, 0.0))
        segments[van.van_id] = segs

    ids = [p["order_id"] for p in pending]
    failed = set(rng.sample(ids, round(fail_rate * len(ids))))
    delivered = [i for i in ids if i not in failed]
    late = set(rng.sample(delivered, round(late_rate * len(delivered))))

    orders: dict[str, OrderTruth] = {}
    events: list[OrderEvent] = []
    for p in pending:
        oid = p["order_id"]
        if oid in late:
            window_end = p["closed"] - rng.randint(5 * 60, 40 * 60)
        else:
            window_end = p["closed"] + rng.randint(10 * 60, 90 * 60)
        window_start = max(window_end - WINDOW_S, p["created"] + 15 * 60)
        if not p["created"] < window_start < window_end:
            raise AssertionError(f"invalid promised window for {oid}")
        outcome = "failed" if oid in failed else "delivered"
        truth = OrderTruth(
            order_id=oid,
            depot_id=p["depot_id"],
            van_id=p["van_id"],
            stop_seq=p["stop_seq"],
            dest_lat=p["dest"][0],
            dest_lon=p["dest"][1],
            created_at=_utc(p["created"]),
            assigned_at=_utc(p["assigned"]),
            out_at=_utc(p["out"]),
            arrived_at=_utc(p["arrival"]),
            closed_at=_utc(p["closed"]),
            outcome=outcome,
            promised_start=_utc(window_start),
            promised_end=_utc(window_end),
        )
        orders[oid] = truth
        events += [
            OrderEvent(
                truth.created_at,
                "created",
                oid,
                {
                    "depot_id": truth.depot_id,
                    "dest_lat": truth.dest_lat,
                    "dest_lon": truth.dest_lon,
                    "promised_start": truth.promised_start,
                    "promised_end": truth.promised_end,
                },
            ),
            OrderEvent(
                truth.assigned_at,
                "assigned",
                oid,
                {"van_id": truth.van_id, "stop_seq": truth.stop_seq},
            ),
            OrderEvent(truth.out_at, "out_for_delivery", oid),
            OrderEvent(
                truth.closed_at,
                outcome,
                oid,
                {"reason": "customer_absent"} if outcome == "failed" else {},
            ),
        ]
    events.sort(key=lambda e: (e.ts, e.order_id, KIND_RANK[e.kind]))
    return DayPlan(
        seed=seed,
        local_date=local_date,
        interval_s=interval_s,
        day_start=day_start,
        van_ids=[v.van_id for v in fleet],
        orders=orders,
        events=events,
        segments=segments,
    )


def local_dates(start: datetime, end: datetime) -> list[date]:
    """Local calendar dates touched by [start, end]."""
    d, last = start.astimezone(LOCAL_TZ).date(), end.astimezone(LOCAL_TZ).date()
    out = []
    while d <= last:
        out.append(d)
        d += timedelta(days=1)
    return out


def merged_stream(
    plans: Iterable[DayPlan],
    start: datetime,
    end: datetime,
    *,
    gps: bool = True,
    orders: bool = True,
) -> Iterator[tuple[datetime, str, OrderEvent | Ping]]:
    """Time-ordered ("order", event) and ("ping", ping) items with start <= ts <= end."""
    streams = []
    for plan in plans:
        if orders:
            streams.append((e.ts, "order", e) for e in plan.events if start <= e.ts <= end)
        if gps:
            streams.append((p.event_ts, "ping", p) for p in plan.pings(start=start, end=end))
    yield from heapq.merge(*streams, key=lambda item: (item[0], item[1] == "ping"))

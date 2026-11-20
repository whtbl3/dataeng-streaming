"""The control tower's rules, per van (spec 006, ADR 0013). Pure Python, no I/O, no Flink.

One `VanState` per van holds its latest position, its recent moving speeds, its open orders and
its alert state. `step(state, event, now)` applies one event and returns what changed: the van's
latest position, order ETAs and alerts. The Flink job keeps one state per van (keyed by van id);
tests replay a whole simulated day through the same function.

Rules (spec 006 resolved questions 3, 4 and 6; BR-3):
- A ping older than (or as old as) the van's latest ping is ignored: positions never move back.
- ETA for each order out for delivery: through the van's remaining stops in stop order
  (`flink.udf.eta`): the current leg at the mean moving speed of the last 10 min, later legs at
  the typical fleet speed.
- SLA risk: ETA >= promised_end + 1 min. Raised once per order; raised again only after it has
  been resolved for >= 10 min. Resolved when ETA <= promised_end, or the order is delivered/failed.
- Abnormal stop: speed < 3 km/h for >= 10 min without a break (no gap over 2 min between pings,
  no move over 150 m), more than 150 m from the depot and from every stop of the van's open
  orders. Resolved when the van moves again.
- Alert horizon: an event older than 10 min (now - event_ts) updates state but raises no alert.
  `now` is processing time in Flink and event time in an offline replay.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from flink.udf.eta import haversine_km, route_eta

SLA_MARGIN = timedelta(minutes=1)
RERAISE_AFTER = timedelta(minutes=10)
IDLE_KMH = 3.0
IDLE_AFTER = timedelta(minutes=10)
AWAY_KM = 0.150
SPEED_WINDOW = timedelta(minutes=10)
MAX_GAP = timedelta(minutes=2)  # a longer gap between pings breaks a still period (offline)
HORIZON = timedelta(minutes=10)
SLA_RISK, ABNORMAL_STOP = "SLA_RISK", "ABNORMAL_STOP"
OPEN = ("assigned", "out_for_delivery")
CLOSED = ("delivered", "failed")


# ---- events in ------------------------------------------------------------------------------
@dataclass(frozen=True)
class Ping:
    van_id: str
    event_ts: datetime
    lat: float
    lon: float
    speed_kmh: float
    depot: tuple[float, float] | None = None


@dataclass(frozen=True)
class OrderChange:
    """One order change for one van. `status` is the order's status after the change; `removed`
    marks the old van of a reassigned order (the order leaves this van)."""

    van_id: str
    order_id: str
    status: str
    stop_seq: int | None
    dest_lat: float
    dest_lon: float
    promised_end: datetime
    event_ts: datetime  # the order system's updated_at
    lsn: int
    removed: bool = False


# ---- outputs --------------------------------------------------------------------------------
@dataclass(frozen=True)
class VehicleLatest:
    van_id: str
    event_ts: datetime
    lat: float
    lon: float
    speed_kmh: float


@dataclass(frozen=True)
class OrderEta:
    order_id: str
    van_id: str
    status: str
    promised_end: datetime
    eta: datetime | None  # None: not dispatched yet
    distance_km: float | None
    event_ts: datetime
    deleted: bool = False  # the order closed or left this van


@dataclass(frozen=True)
class Alert:
    alert_id: str
    type: str
    van_id: str
    order_id: str | None
    raised_at: datetime  # event time of the ping (or change) that raised it
    resolved_at: datetime | None
    lat: float
    lon: float
    detail: str
    event: str  # "raised" or "resolved"


# ---- state ----------------------------------------------------------------------------------
@dataclass
class OpenOrder:
    order_id: str
    status: str
    stop_seq: int | None
    dest: tuple[float, float]
    promised_end: datetime
    lsn: int


@dataclass
class SlaState:
    open: Alert | None = None
    resolved_at: datetime | None = None


@dataclass
class VanState:
    latest: Ping | None = None
    speeds: deque = field(default_factory=deque)  # (event_ts, speed) of moving pings
    orders: dict[str, OpenOrder] = field(default_factory=dict)
    sla: dict[str, SlaState] = field(default_factory=dict)
    depot: tuple[float, float] | None = None
    still_since: datetime | None = None
    still_at: tuple[float, float] | None = None
    stop_alert: Alert | None = None
    stop_alerted: bool = False  # raised once per still period


def _fresh(event_ts: datetime, now: datetime) -> bool:
    return now - event_ts <= HORIZON


def _mean_speed(state: VanState) -> float | None:
    return sum(s for _, s in state.speeds) / len(state.speeds) if state.speeds else None


def _dispatched(state: VanState) -> list[OpenOrder]:
    out = [o for o in state.orders.values() if o.status == "out_for_delivery"]
    last = 1 << 30  # an order without a stop number goes last
    return sorted(out, key=lambda o: (o.stop_seq if o.stop_seq is not None else last, o.order_id))


def _resolve(alert: Alert, at: datetime) -> Alert:
    return Alert(
        alert.alert_id, alert.type, alert.van_id, alert.order_id, alert.raised_at, at,
        alert.lat, alert.lon, alert.detail, "resolved",
    )  # fmt: skip


def _etas(state: VanState, van_id: str, now: datetime, at: datetime | None = None) -> list:
    """Recompute every open order's ETA from the latest ping; evaluate SLA risk. Without any
    ping yet, open orders are still listed, with no ETA (`at` is the change's event time)."""
    ping = state.latest
    if ping is None:
        return [
            OrderEta(o.order_id, van_id, o.status, o.promised_end, None, None, at)
            for o in state.orders.values()
        ]
    out: list = []
    speed = _mean_speed(state)
    route = _dispatched(state)
    for i, order in enumerate(route):
        stops = [o.dest for o in route[: i + 1]]
        eta, km = route_eta(ping.event_ts, (ping.lat, ping.lon), speed, stops)
        out.append(
            OrderEta(order.order_id, van_id, order.status, order.promised_end, eta,
                     round(km, 3), ping.event_ts)
        )  # fmt: skip
        out += _sla(state, van_id, order, eta, ping, now)
    for order in state.orders.values():
        if order.status != "out_for_delivery":
            out.append(
                OrderEta(order.order_id, van_id, order.status, order.promised_end, None, None,
                         ping.event_ts)
            )  # fmt: skip
    return out


def _sla(state: VanState, van_id: str, order: OpenOrder, eta: datetime, ping: Ping, now) -> list:
    sla = state.sla.setdefault(order.order_id, SlaState())
    at = ping.event_ts
    if sla.open is None:
        late_by = eta - order.promised_end
        cooled = sla.resolved_at is None or at - sla.resolved_at >= RERAISE_AFTER
        if late_by >= SLA_MARGIN and cooled and _fresh(at, now):
            minutes = round(late_by.total_seconds() / 60)
            sla.open = Alert(
                f"{SLA_RISK}:{order.order_id}:{int(at.timestamp())}", SLA_RISK, van_id,
                order.order_id, at, None, ping.lat, ping.lon,
                f"ETA {eta:%H:%M}Z is {minutes} min after the promised end", "raised",
            )  # fmt: skip
            return [sla.open]
        return []
    if eta <= order.promised_end:
        resolved = _resolve(sla.open, at)
        sla.open, sla.resolved_at = None, at
        return [resolved]
    return []


def _away(state: VanState, point: tuple[float, float]) -> bool:
    places = [o.dest for o in state.orders.values()]
    if state.depot is not None:
        places.append(state.depot)
    return all(haversine_km(*point, *p) > AWAY_KM for p in places)


def _add_speed(state: VanState, ping: Ping, newest: datetime) -> None:
    """Keep the moving speeds of the last 10 min of event time, one per ping time."""
    if ping.speed_kmh >= IDLE_KMH and newest - ping.event_ts <= SPEED_WINDOW:
        times = {ts for ts, _ in state.speeds}
        if ping.event_ts not in times:
            state.speeds.append((ping.event_ts, ping.speed_kmh))
            state.speeds = deque(sorted(state.speeds))
    while state.speeds and newest - state.speeds[0][0] > SPEED_WINDOW:
        state.speeds.popleft()


def _on_ping(state: VanState, ping: Ping, now: datetime) -> list:
    latest = state.latest
    if latest is not None and ping.event_ts <= latest.event_ts:
        # Older or duplicate: never move the van back (AC3). An out-of-order ping still counts
        # for the recent speed, so jitter does not change the ETA (AC8).
        _add_speed(state, ping, latest.event_ts)
        return []
    state.latest = ping
    if ping.depot is not None:
        state.depot = ping.depot
    _add_speed(state, ping, ping.event_ts)
    out: list = [VehicleLatest(ping.van_id, ping.event_ts, ping.lat, ping.lon, ping.speed_kmh)]

    if ping.speed_kmh >= IDLE_KMH:
        state.still_since, state.still_at, state.stop_alerted = None, None, False
        if state.stop_alert is not None:
            out.append(_resolve(state.stop_alert, ping.event_ts))
            state.stop_alert = None
    else:
        here = (ping.lat, ping.lon)
        # Still means still, continuously: a gap without pings (offline) or a new place starts
        # a new still period (otherwise two short stops around an offline burst add up).
        gap = latest is None or ping.event_ts - latest.event_ts > MAX_GAP
        moved = state.still_at is not None and haversine_km(*here, *state.still_at) > AWAY_KM
        if state.still_since is None or gap or moved:
            state.still_since, state.still_at, state.stop_alerted = ping.event_ts, here, False
        idle = ping.event_ts - state.still_since
        if (
            idle >= IDLE_AFTER
            and not state.stop_alerted
            and _fresh(ping.event_ts, now)
            and _away(state, state.still_at)
        ):
            minutes = round(idle.total_seconds() / 60)
            state.stop_alert = Alert(
                f"{ABNORMAL_STOP}:{ping.van_id}:{int(state.still_since.timestamp())}",
                ABNORMAL_STOP, ping.van_id, None, ping.event_ts, None, *state.still_at,
                f"idle {minutes} min away from its stops and depot", "raised",
            )  # fmt: skip
            state.stop_alerted = True
            out.append(state.stop_alert)
    return out + _etas(state, ping.van_id, now)


def _on_order(state: VanState, change: OrderChange, now: datetime) -> list:
    current = state.orders.get(change.order_id)
    if current is not None and change.lsn <= current.lsn:
        return []  # duplicate or older change
    out: list = []
    if change.removed or change.status in CLOSED:
        state.orders.pop(change.order_id, None)
        sla = state.sla.pop(change.order_id, None)
        if sla is not None and sla.open is not None:
            out.append(_resolve(sla.open, change.event_ts))
        out.append(
            OrderEta(change.order_id, change.van_id, change.status, change.promised_end, None,
                     None, change.event_ts, deleted=True)
        )  # fmt: skip
        return out
    if change.status not in OPEN:
        return []
    state.orders[change.order_id] = OpenOrder(
        change.order_id, change.status, change.stop_seq, (change.dest_lat, change.dest_lon),
        change.promised_end, change.lsn,
    )  # fmt: skip
    return _etas(state, change.van_id, now, change.event_ts)


def step(state: VanState, event: Ping | OrderChange, now: datetime) -> list:
    """Apply one event to the van's state; return the outputs it causes."""
    if isinstance(event, Ping):
        return _on_ping(state, event, now)
    return _on_order(state, event, now)

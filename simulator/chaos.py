"""Chaos mode (spec 004): send the same simulated day dirty, in exactly known amounts.

Real GPS devices resend pings they are not sure were delivered, lose signal and flush old
pings when they reconnect, and reach the broker out of order. `Chaos.apply` takes the clean
time-ordered stream from `simulator.day.merged_stream` and returns it in *send* order:

- `duplicate`: the ping is sent on time plus 1-3 extra copies, 1-30 s later.
- `delayed`:   sent 1-10 min late (90%) or 10 min-`max_delay_min` late (10%).
- `jitter`:    sent 0-10 s late, which reorders it across the 5 s ping interval.
- `offline`:   one 2-15 min window per van and local day (between 08:00 and 18:00); its
               pings are flushed in event order right after the window ends.
- `clean`:     sent at its event time.

Each decision is a pure function of (seed, vehicle id, event time), so it does not depend on
where the stream starts, and the set of distinct pings always equals the clean run (AC1).
Order events pass through at their own time. Pure logic, no I/O.
"""

from __future__ import annotations

import heapq
import zlib
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from simulator.day import LOCAL_TZ, _splitmix64
from simulator.fleet import Ping

KINDS = ("clean", "duplicate", "delayed", "jitter", "offline")
OFFLINE_FROM_H, OFFLINE_TO_H = 8, 18
OFFLINE_LEN_S = (120, 900)
FLUSH_STEP_MS = 10  # per second of offline time, keeps the flush in event order
SHORT_DELAY_S = (60, 600)
LONG_DELAY_SHARE = 0.10
JITTER_MAX_S = 10.0
COPY_DELAY_S = (1, 30)


@dataclass(frozen=True)
class ChaosConfig:
    dup_rate: float = 0.0
    delay_rate: float = 0.0
    max_delay_min: float = 120.0
    jitter_rate: float = 0.0
    offline: bool = False

    def __post_init__(self) -> None:
        rates = (self.dup_rate, self.delay_rate, self.jitter_rate)
        if any(r < 0 or r > 1 for r in rates) or sum(rates) > 1:
            raise ValueError("chaos rates must be in [0, 1] and sum to at most 1")
        if self.delay_rate > 0 and self.max_delay_min * 60 <= SHORT_DELAY_S[0]:
            raise ValueError("max_delay_min must be more than 1 minute")

    @property
    def enabled(self) -> bool:
        return self.offline or any((self.dup_rate, self.delay_rate, self.jitter_rate))


CHAOS_DEFAULTS = ChaosConfig(dup_rate=0.05, delay_rate=0.05, jitter_rate=0.10, offline=True)


class Chaos:
    def __init__(self, config: ChaosConfig, seed: int) -> None:
        self.config = config
        self.seed = seed
        self.counts: Counter[str] = Counter({k: 0 for k in (*KINDS, "copies")})
        self._windows: dict[tuple[str, date], tuple[datetime, datetime]] = {}

    @property
    def offline_windows(self) -> list[tuple[str, datetime, datetime]]:
        return [(van, lo, hi) for (van, _), (lo, hi) in sorted(self._windows.items())]

    # -- deterministic draws ------------------------------------------------------------------
    def _u(self, vehicle_id: str, epoch_ms: int, salt: int) -> float:
        """Uniform [0, 1) from (seed, van, time, salt)."""
        key = (self.seed * 1_000_003 + zlib.crc32(vehicle_id.encode())) * 7_919 + salt
        x = _splitmix64((key ^ _splitmix64(epoch_ms)) & 0xFFFFFFFFFFFFFFFF)
        return (x >> 11) / float(1 << 53)

    def _window(self, vehicle_id: str, ts: datetime) -> tuple[datetime, datetime]:
        local_day = ts.astimezone(LOCAL_TZ).date()
        key = (vehicle_id, local_day)
        if key not in self._windows:
            day_ms = local_day.toordinal()
            length = OFFLINE_LEN_S[0] + self._u(vehicle_id, day_ms, 11) * (
                OFFLINE_LEN_S[1] - OFFLINE_LEN_S[0]
            )
            span = (OFFLINE_TO_H - OFFLINE_FROM_H) * 3600 - length
            begin = datetime.combine(local_day, datetime.min.time(), LOCAL_TZ) + timedelta(
                hours=OFFLINE_FROM_H, seconds=int(self._u(vehicle_id, day_ms, 12) * span)
            )
            self._windows[key] = (begin, begin + timedelta(seconds=int(length)))
        return self._windows[key]

    def sends(self, ping: Ping) -> tuple[str, list[datetime]]:
        """The chaos kind of one ping and every time it is sent (first one first)."""
        cfg, ts, van = self.config, ping.event_ts, ping.vehicle_id
        ms = int(ts.timestamp() * 1000)
        if cfg.offline:
            lo, hi = self._window(van, ts)
            if lo <= ts < hi:
                step = timedelta(milliseconds=FLUSH_STEP_MS * (ts - lo).total_seconds())
                return "offline", [hi + step]
        u = self._u(van, ms, 1)
        if u < cfg.dup_rate:
            n = 1 + int(self._u(van, ms, 2) * 3)
            extra = [
                ts + timedelta(seconds=COPY_DELAY_S[0] + self._u(van, ms, 3 + i) * 29)
                for i in range(n)
            ]
            return "duplicate", [ts, *sorted(extra)]
        u -= cfg.dup_rate
        if u < cfg.delay_rate:
            if self._u(van, ms, 7) < LONG_DELAY_SHARE:
                lo_s, hi_s = SHORT_DELAY_S[1], cfg.max_delay_min * 60
            else:
                lo_s, hi_s = SHORT_DELAY_S[0], min(SHORT_DELAY_S[1], cfg.max_delay_min * 60)
            return "delayed", [ts + timedelta(seconds=lo_s + self._u(van, ms, 8) * (hi_s - lo_s))]
        u -= cfg.delay_rate
        if u < cfg.jitter_rate:
            return "jitter", [ts + timedelta(seconds=self._u(van, ms, 9) * JITTER_MAX_S)]
        return "clean", [ts]

    # -- stream -------------------------------------------------------------------------------
    def apply(
        self, stream: Iterator[tuple[datetime, str, object]], until: datetime | None = None
    ) -> Iterator[tuple[datetime, str, object]]:
        """Re-time a clean (ts, kind, item) stream into send order.

        `until` clamps send times (history mode cannot send in the future); nothing is lost.
        """
        if not self.config.enabled:
            yield from stream
            return
        heap: list[tuple[datetime, int, str, object]] = []
        seq = 0
        for ts, kind, item in stream:
            if kind == "ping":
                chaos_kind, times = self.sends(item)
                self.counts[chaos_kind] += 1
                self.counts["copies"] += len(times) - 1
            else:
                times = [ts]
            for t in times:
                heapq.heappush(heap, (t, seq, kind, item))
                seq += 1
            while heap and heap[0][0] <= ts:
                t, _, k, it = heapq.heappop(heap)
                yield t, k, it
        while heap:
            t, _, k, it = heapq.heappop(heap)
            yield (min(t, until) if until else t), k, it

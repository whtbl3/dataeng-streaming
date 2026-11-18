"""Deterministic GPS ping generator: vans do a bounded random walk inside Ho Chi Minh City.

Pure logic, no I/O, so it is unit tested without Kafka. Event times are UTC and step
by exactly `interval_s` per vehicle, ending at `end_ts`.
"""

from __future__ import annotations

import math
import random
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

MAX_SPEED_KMH = 80.0
KM_PER_DEG_LAT = 111.32
STOP_PROBABILITY = 0.05  # chance per ping that a moving van stops (delivery, traffic light)


@dataclass(frozen=True)
class BBox:
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float


HCMC_BBOX = BBox(lat_min=10.65, lat_max=10.90, lon_min=106.55, lon_max=106.85)


@dataclass(frozen=True)
class Ping:
    vehicle_id: str
    event_ts: datetime
    lat: float
    lon: float
    speed_kmh: float
    heading_deg: float

    def to_dict(self) -> dict[str, object]:
        return {
            "vehicle_id": self.vehicle_id,
            "event_ts": self.event_ts.strftime("%Y-%m-%dT%H:%M:%S.")
            + f"{self.event_ts.microsecond // 1000:03d}Z",
            "lat": self.lat,
            "lon": self.lon,
            "speed_kmh": self.speed_kmh,
            "heading_deg": self.heading_deg,
        }


@dataclass
class _VanState:
    lat: float
    lon: float
    speed_kmh: float
    heading_deg: float


def _reflect(value: float, low: float, high: float) -> tuple[float, bool]:
    """Mirror `value` back into [low, high]; return (value, reflected?)."""
    if value < low:
        return min(2 * low - value, high), True
    if value > high:
        return max(2 * high - value, low), True
    return value, False


def _step(state: _VanState, rng: random.Random, interval_s: int, bbox: BBox) -> None:
    if state.speed_kmh > 0 and rng.random() < STOP_PROBABILITY:
        state.speed_kmh = 0.0
    else:
        target = state.speed_kmh + rng.gauss(0, 6) + (15 if state.speed_kmh == 0 else 0)
        state.speed_kmh = min(max(target, 0.0), MAX_SPEED_KMH)
    state.heading_deg = (state.heading_deg + rng.gauss(0, 20)) % 360.0

    dist_km = state.speed_kmh * interval_s / 3600
    rad = math.radians(state.heading_deg)
    new_lat = state.lat + dist_km * math.cos(rad) / KM_PER_DEG_LAT
    new_lon = state.lon + dist_km * math.sin(rad) / (
        KM_PER_DEG_LAT * math.cos(math.radians(state.lat))
    )

    new_lat, hit_lat = _reflect(new_lat, bbox.lat_min, bbox.lat_max)
    new_lon, hit_lon = _reflect(new_lon, bbox.lon_min, bbox.lon_max)
    if hit_lat:
        state.heading_deg = (180.0 - state.heading_deg) % 360.0
    if hit_lon:
        state.heading_deg = (360.0 - state.heading_deg) % 360.0
    state.lat, state.lon = new_lat, new_lon


def _snapshot(vehicle_id: str, ts: datetime, s: _VanState, bbox: BBox) -> Ping:
    lat = min(max(round(s.lat, 6), bbox.lat_min), bbox.lat_max)
    lon = min(max(round(s.lon, 6), bbox.lon_min), bbox.lon_max)
    heading = round(s.heading_deg, 1) % 360.0
    return Ping(vehicle_id, ts, lat, lon, round(s.speed_kmh, 1), heading)


def generate_pings(
    *,
    seed: int,
    vans: int,
    interval_s: int,
    steps: int,
    end_ts: datetime,
    prefix: str = "VAN",
    bbox: BBox = HCMC_BBOX,
) -> Iterator[Ping]:
    """Yield `vans * steps` pings, time-ordered (all vans per step), last step at `end_ts`."""
    if vans < 1 or steps < 1 or interval_s < 1:
        raise ValueError("vans, steps and interval_s must be >= 1")
    if end_ts.tzinfo is None or end_ts.utcoffset() != timedelta(0):
        raise ValueError("end_ts must be a timezone-aware UTC datetime")

    rng = random.Random(seed)
    ids = [f"{prefix}-{i:04d}" for i in range(1, vans + 1)]
    states = [
        _VanState(
            lat=rng.uniform(bbox.lat_min, bbox.lat_max),
            lon=rng.uniform(bbox.lon_min, bbox.lon_max),
            speed_kmh=rng.uniform(0, 40),
            heading_deg=rng.uniform(0, 360),
        )
        for _ in ids
    ]
    start = end_ts - timedelta(seconds=interval_s * (steps - 1))
    for step in range(steps):
        ts = start + timedelta(seconds=interval_s * step)
        for vid, state in zip(ids, states, strict=True):
            if step > 0:
                _step(state, rng, interval_s, bbox)
            yield _snapshot(vid, ts, state, bbox)

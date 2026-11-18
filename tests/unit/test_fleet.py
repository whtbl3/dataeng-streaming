"""GPS properties from spec 001 (AC2), now checked on the coupled day simulator (spec 002)."""

import math
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta

from simulator.day import LOCAL_TZ, simulate_day
from simulator.fleet import HCMC_BBOX, MAX_SPEED_KMH

DAY = date(2026, 11, 18)
# 06:00-16:00 local covers parking, both delivery waves and returns
START = datetime(2026, 11, 18, 6, 0, tzinfo=LOCAL_TZ).astimezone(UTC)
END = datetime(2026, 11, 18, 16, 0, tzinfo=LOCAL_TZ).astimezone(UTC)


def _pings(seed: int = 42, vans: int = 5):
    plan = simulate_day(seed=seed, local_date=DAY, vans=vans, orders_per_day=vans * 24)
    return list(plan.pings(start=START, end=END))


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def test_count_is_vans_times_ticks():
    ticks = int((END - START).total_seconds() // 5) + 1
    assert len(_pings(vans=3)) == 3 * ticks


def test_every_ping_has_vehicle_id_and_utc_time():
    for p in _pings():
        assert p.vehicle_id.startswith("VAN-")
        assert p.event_ts.utcoffset() == timedelta(0)


def test_positions_stay_inside_hcmc_bbox():
    for p in _pings():
        assert HCMC_BBOX.lat_min <= p.lat <= HCMC_BBOX.lat_max
        assert HCMC_BBOX.lon_min <= p.lon <= HCMC_BBOX.lon_max


def test_speed_and_heading_are_plausible():
    pings = _pings()
    for p in pings:
        assert 0.0 <= p.speed_kmh <= MAX_SPEED_KMH
        assert 0.0 <= p.heading_deg < 360.0
    assert any(p.speed_kmh > 10 for p in pings)  # vans actually drive


def test_event_times_step_by_interval():
    by_van = defaultdict(list)
    for p in _pings():
        by_van[p.vehicle_id].append(p.event_ts)
    for times in by_van.values():
        assert all(b - a == timedelta(seconds=5) for a, b in zip(times, times[1:], strict=False))


def test_movement_between_pings_matches_speed_limit():
    by_van = defaultdict(list)
    for p in _pings():
        by_van[p.vehicle_id].append(p)
    max_km = MAX_SPEED_KMH * 5 / 3600
    for pts in by_van.values():
        for a, b in zip(pts, pts[1:], strict=False):
            assert _haversine_km(a.lat, a.lon, b.lat, b.lon) <= max_km


def test_same_seed_gives_identical_output_and_different_seed_differs():
    assert _pings(seed=7, vans=2) == _pings(seed=7, vans=2)
    assert _pings(seed=7, vans=2) != _pings(seed=8, vans=2)

"""AC2: the simulator produces valid, deterministic GPS pings (no Docker needed)."""

import math
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pytest

from simulator.fleet import HCMC_BBOX, MAX_SPEED_KMH, generate_pings

END = datetime(2026, 11, 18, 3, 0, 0, tzinfo=UTC)


def _pings(seed: int = 42, vans: int = 5, steps: int = 50, interval_s: int = 5):
    return list(
        generate_pings(seed=seed, vans=vans, interval_s=interval_s, steps=steps, end_ts=END)
    )


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def test_count_is_vans_times_steps():
    assert len(_pings(vans=7, steps=13)) == 7 * 13


def test_every_ping_has_required_fields_and_utc_time():
    for p in _pings():
        assert p.vehicle_id.startswith("VAN-")
        assert p.event_ts.tzinfo is not None
        assert p.event_ts.utcoffset() == timedelta(0)


def test_positions_stay_inside_hcmc_bbox():
    for p in _pings(steps=500):
        assert HCMC_BBOX.lat_min <= p.lat <= HCMC_BBOX.lat_max
        assert HCMC_BBOX.lon_min <= p.lon <= HCMC_BBOX.lon_max


def test_speed_is_plausible():
    for p in _pings(steps=500):
        assert 0.0 <= p.speed_kmh <= MAX_SPEED_KMH
        assert 0.0 <= p.heading_deg < 360.0


def test_event_times_step_by_interval_and_end_at_end_ts():
    by_van = defaultdict(list)
    for p in _pings(interval_s=5):
        by_van[p.vehicle_id].append(p.event_ts)
    assert len(by_van) == 5
    for times in by_van.values():
        assert times[-1] == END
        assert all(b - a == timedelta(seconds=5) for a, b in zip(times, times[1:], strict=False))


def test_movement_between_pings_matches_speed_limit():
    by_van = defaultdict(list)
    for p in _pings(steps=200, interval_s=5):
        by_van[p.vehicle_id].append(p)
    max_km = MAX_SPEED_KMH * 5 / 3600
    for pts in by_van.values():
        for a, b in zip(pts, pts[1:], strict=False):
            # small tolerance for edge reflection and float error
            assert _haversine_km(a.lat, a.lon, b.lat, b.lon) <= max_km * 1.05


def test_same_seed_gives_identical_output_and_different_seed_differs():
    assert _pings(seed=7) == _pings(seed=7)
    assert _pings(seed=7) != _pings(seed=8)


def test_custom_prefix():
    ids = {
        p.vehicle_id
        for p in generate_pings(seed=1, vans=2, interval_s=5, steps=1, end_ts=END, prefix="E2E-abc")
    }
    assert ids == {"E2E-abc-0001", "E2E-abc-0002"}


@pytest.mark.parametrize(
    "kwargs",
    [
        {"vans": 0},
        {"steps": 0},
        {"interval_s": 0},
        {"end_ts": datetime(2026, 1, 1)},  # naive datetime
    ],
)
def test_invalid_arguments_raise(kwargs):
    args = {"seed": 1, "vans": 1, "interval_s": 5, "steps": 1, "end_ts": END} | kwargs
    with pytest.raises(ValueError):
        list(generate_pings(**args))

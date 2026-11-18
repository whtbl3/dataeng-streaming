"""GPS validation rules (spec 003 AC4). Late events are valid; only the 4 rules reject."""

from datetime import UTC, datetime, timedelta

import pytest

from contracts.validate import gps_violation

INGEST = datetime(2026, 11, 18, 1, 0, 0, tzinfo=UTC)
GOOD = {
    "vehicle_id": "VAN-0001",
    "event_ts": INGEST - timedelta(hours=2),
    "lat": 10.78,
    "lon": 106.69,
    "speed_kmh": 37.5,
    "heading_deg": 309.1,
}


def test_valid_ping_passes_even_when_two_hours_late():
    assert gps_violation(GOOD, INGEST) is None


@pytest.mark.parametrize(
    "change, reason",
    [
        ({"vehicle_id": ""}, "missing_vehicle_id"),
        ({"vehicle_id": None}, "missing_vehicle_id"),
        ({"vehicle_id": "  "}, "missing_vehicle_id"),
        ({"lat": 21.03}, "out_of_bbox"),
        ({"lon": 105.8}, "out_of_bbox"),
        ({"lat": None}, "out_of_bbox"),
        ({"speed_kmh": 150.0}, "speed_out_of_range"),
        ({"speed_kmh": -1.0}, "speed_out_of_range"),
        ({"speed_kmh": float("nan")}, "speed_out_of_range"),
        ({"event_ts": INGEST + timedelta(minutes=10)}, "event_ts_in_future"),
    ],
)
def test_each_rule_has_its_own_reason(change, reason):
    got = gps_violation({**GOOD, **change}, INGEST)
    assert got is not None and got[0] == reason and got[1]


def test_boundaries_are_inclusive():
    edge = {
        **GOOD,
        "lat": 10.65,
        "lon": 106.85,
        "speed_kmh": 80.0,
        "event_ts": INGEST + timedelta(minutes=5),
    }
    assert gps_violation(edge, INGEST) is None

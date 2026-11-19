"""ETA heuristic v1 (spec 006 AC2, resolved question 3 as revised)."""

from datetime import UTC, datetime, timedelta

import pytest

from flink.udf.eta import DWELL, TYPICAL_SPEED_KMH, haversine_km, route_eta

T0 = datetime(2026, 11, 19, 3, 0, tzinfo=UTC)
A = (10.7769, 106.7009)  # Ben Thanh area
B = (10.7769 + 0.09, 106.7009)  # ~10 km north
C = (10.7769 + 0.18, 106.7009)  # ~20 km north


def test_haversine_one_tenth_degree_of_latitude_is_about_11_km():
    assert haversine_km(*A, *B) == pytest.approx(10.0, abs=0.1)
    assert haversine_km(*A, *A) == 0


def test_one_stop_at_the_current_speed():
    eta, km = route_eta(T0, A, 40.0, [B])
    assert km == pytest.approx(10.0, abs=0.1)
    assert eta == pytest.approx(T0 + timedelta(hours=km / 40.0), abs=timedelta(seconds=1))


def test_later_legs_use_the_typical_speed_and_each_earlier_stop_adds_a_dwell():
    eta, km = route_eta(T0, A, 10.0, [B, C])  # jammed now (10 km/h); the next leg is typical
    first, second = haversine_km(*A, *B), haversine_km(*B, *C)
    expected = T0 + timedelta(hours=first / 10.0 + second / TYPICAL_SPEED_KMH) + DWELL
    assert eta == pytest.approx(expected, abs=timedelta(seconds=1))
    assert km == pytest.approx(first + second)


@pytest.mark.parametrize("speed", [None, 0.0])
def test_without_a_current_speed_every_leg_is_typical(speed):
    eta, km = route_eta(T0, A, speed, [B])
    assert eta == pytest.approx(
        T0 + timedelta(hours=km / TYPICAL_SPEED_KMH), abs=timedelta(seconds=1)
    )

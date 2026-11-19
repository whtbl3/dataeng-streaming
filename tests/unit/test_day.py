"""Spec 002 AC2-AC4: the coupled day simulation (orders + GPS tell the same story)."""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta

import pytest

from simulator.day import LOCAL_TZ, simulate_day
from simulator.world import DEPOT_BY_ID, VANS

DAY = date(2026, 11, 18)
LIFECYCLE_OK = (
    ["created", "assigned", "out_for_delivery", "delivered"],
    ["created", "assigned", "out_for_delivery", "failed"],
)


def _km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


@pytest.fixture(scope="module")
def plan():
    return simulate_day(seed=42, local_date=DAY, vans=8, orders_per_day=200)


def _events_by_order(plan):
    by_order = defaultdict(list)
    for e in plan.events:
        by_order[e.order_id].append(e)
    return by_order


def test_orders_per_day_and_global_time_order(plan):
    assert len(plan.orders) == 200
    ts = [e.ts for e in plan.events]
    assert ts == sorted(ts)
    assert all(t.tzinfo is not None and t.utcoffset() == timedelta(0) for t in ts)


def test_lifecycle_valid_ordered_and_depot_consistent(plan):
    van_depot = {v.van_id: v.depot_id for v in VANS}
    for order_id, events in _events_by_order(plan).items():
        assert [e.kind for e in events] in LIFECYCLE_OK, order_id
        times = [e.ts for e in events]
        assert times == sorted(times), order_id
        created = events[0]
        assigned = events[1]
        assert created.fields["promised_start"] > created.ts
        assert created.fields["promised_end"] > created.fields["promised_start"]
        assert van_depot[assigned.fields["van_id"]] == created.fields["depot_id"]
        assert assigned.fields["stop_seq"] >= 1


def test_deterministic():
    a = simulate_day(seed=7, local_date=DAY, vans=4, orders_per_day=40)
    b = simulate_day(seed=7, local_date=DAY, vans=4, orders_per_day=40)
    c = simulate_day(seed=8, local_date=DAY, vans=4, orders_per_day=40)
    assert a.events == b.events
    assert list(a.pings()) == list(b.pings())
    assert a.events != c.events


def test_van_at_destination_when_order_closes(plan):
    pings = {(p.vehicle_id, p.event_ts): p for p in plan.pings()}
    for order in plan.orders.values():
        tick = plan.tick_at_or_before(order.closed_at)
        p = pings[(order.van_id, tick)]
        assert _km(p.lat, p.lon, order.dest_lat, order.dest_lon) <= 0.05, order.order_id
        assert p.speed_kmh == 0.0


def test_shift_starts_and_ends_at_depot(plan):
    first, last = {}, {}
    for p in plan.pings():
        first.setdefault(p.vehicle_id, p)
        last[p.vehicle_id] = p
    for van in VANS[:8]:
        depot = DEPOT_BY_ID[van.depot_id]
        for p in (first[van.van_id], last[van.van_id]):
            assert _km(p.lat, p.lon, depot.lat, depot.lon) <= 0.05, van.van_id
            assert p.speed_kmh == 0.0


def test_late_and_failed_rates_default_and_configurable():
    default = simulate_day(seed=1, local_date=DAY, vans=40, orders_per_day=2000)
    _assert_rates(default, late=0.10, failed=0.02)
    custom = simulate_day(
        seed=1, local_date=DAY, vans=40, orders_per_day=2000, late_rate=0.3, fail_rate=0.05
    )
    _assert_rates(custom, late=0.30, failed=0.05)


def _assert_rates(plan, *, late: float, failed: float) -> None:
    orders = list(plan.orders.values())
    delivered = [o for o in orders if o.outcome == "delivered"]
    n_failed = sum(o.outcome == "failed" for o in orders)
    n_late = sum(o.closed_at > o.promised_end for o in delivered)
    assert abs(n_late / len(delivered) - late) <= 0.03
    assert abs(n_failed / len(orders) - failed) <= 0.03


def test_pings_cover_the_whole_local_day_every_interval():
    plan = simulate_day(seed=3, local_date=DAY, vans=2, orders_per_day=10, interval_s=60)
    pings = list(plan.pings())
    assert len(pings) == 2 * 24 * 60
    start = datetime(2026, 11, 18, tzinfo=LOCAL_TZ).astimezone(UTC)
    assert pings[0].event_ts == start
    assert pings[-1].event_ts == start + timedelta(days=1) - timedelta(minutes=1)


def test_pings_window_and_vehicle_prefix():
    plan = simulate_day(seed=3, local_date=DAY, vans=3, orders_per_day=10)
    start = datetime(2026, 11, 18, 9, 0, tzinfo=LOCAL_TZ).astimezone(UTC)
    end = start + timedelta(seconds=5 * 9)
    pings = list(plan.pings(start=start, end=end, vehicle_prefix="E2E-x"))
    assert len(pings) == 3 * 10
    assert {p.vehicle_id for p in pings} == {"E2E-x-0001", "E2E-x-0002", "E2E-x-0003"}


def test_invalid_arguments():
    with pytest.raises(ValueError):
        simulate_day(seed=1, local_date=DAY, vans=0, orders_per_day=10)
    with pytest.raises(ValueError):
        simulate_day(seed=1, local_date=DAY, vans=201, orders_per_day=10)
    with pytest.raises(ValueError):
        simulate_day(seed=1, local_date=DAY, vans=1, orders_per_day=10, late_rate=1.5)


# ---- spec 006 AC1: injected abnormal stops ---------------------------------------------------
def _dist(a, b):
    return _km(*a, *b)


def test_abnormal_stops_are_injected_in_known_number_place_and_length():
    clean = simulate_day(seed=5, local_date=date(2026, 11, 18), vans=20, orders_per_day=500)
    dirty = simulate_day(
        seed=5, local_date=date(2026, 11, 18), vans=20, orders_per_day=500, abnormal_stops=3
    )
    assert clean.abnormal_stops == ()
    assert len(dirty.abnormal_stops) == 3
    assert len({s.van_id for s in dirty.abnormal_stops}) == 3  # one per van
    for stop in dirty.abnormal_stops:
        assert 12 <= stop.minutes <= 25
        here = (stop.lat, stop.lon)
        depot = DEPOT_BY_ID[next(v.depot_id for v in VANS if v.van_id == stop.van_id)]
        assert _dist(here, (depot.lat, depot.lon)) > 0.15
        for o in dirty.orders.values():
            if o.van_id == stop.van_id:
                assert _dist(here, (o.dest_lat, o.dest_lon)) > 0.15
                assert o.closed_at <= stop.start  # after the last delivery: orders unaffected
        pings = [
            p
            # inside the stop: a ping exactly on a boundary belongs to the drive (as everywhere)
            for p in dirty.pings(
                start=stop.start + timedelta(seconds=5),
                end=stop.start + timedelta(minutes=stop.minutes, seconds=-5),
            )
            if p.vehicle_id == stop.van_id
        ]
        assert pings and all(p.speed_kmh == 0 for p in pings)
        assert all(_dist((p.lat, p.lon), here) < 0.02 for p in pings)  # GPS jitter only


def test_abnormal_stops_leave_orders_and_other_vans_unchanged():
    clean = simulate_day(seed=5, local_date=date(2026, 11, 18), vans=10, orders_per_day=250)
    dirty = simulate_day(
        seed=5, local_date=date(2026, 11, 18), vans=10, orders_per_day=250, abnormal_stops=2
    )
    assert dirty.orders == clean.orders and dirty.events == clean.events
    stopped = {s.van_id for s in dirty.abnormal_stops}
    for van in clean.van_ids:
        if van not in stopped:
            assert dirty.segments[van] == clean.segments[van]


def test_abnormal_stops_are_deterministic_and_bounded():
    kw = dict(seed=9, local_date=date(2026, 11, 18), vans=4, orders_per_day=100)
    assert (
        simulate_day(**kw, abnormal_stops=2).abnormal_stops
        == simulate_day(**kw, abnormal_stops=2).abnormal_stops
    )
    with pytest.raises(ValueError, match="abnormal_stops"):
        simulate_day(**kw, abnormal_stops=5)  # at most one per van

"""A whole simulated day through the control tower rules, offline (spec 006 AC5, AC6, AC8).

The same `flink.udf.tower.step` the Flink job runs, one state per van, events in arrival order.
`now` is the arrival (send) time, so the alert horizon behaves as it does live.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from flink.udf import tower as t
from simulator.chaos import CHAOS_DEFAULTS, Chaos
from simulator.day import merged_stream, simulate_day
from simulator.world import DEPOT_BY_ID, VANS

DAY = date(2026, 11, 18)
DEPOT = {v.van_id: (DEPOT_BY_ID[v.depot_id].lat, DEPOT_BY_ID[v.depot_id].lon) for v in VANS}
LEAD = timedelta(minutes=15)


PEAKS: dict[int, dict[str, timedelta]] = {}  # id(alerts list) → order → max(ETA - promised end)


def replay(plan, items):
    """Feed (arrival, kind, item) into one VanState per van; return every alert output. The
    highest ETA - promised end per order is kept in PEAKS (how close it came to an alert)."""
    states: dict[str, t.VanState] = {}
    out: list[t.Alert] = []
    peaks: dict[str, timedelta] = {}
    lsn = 0
    for arrival, kind, item in items:
        if kind == "ping":
            event = t.Ping(item.vehicle_id, item.event_ts, item.lat, item.lon, item.speed_kmh,
                           DEPOT[item.vehicle_id])  # fmt: skip
        elif item.kind == "created":
            continue  # no van yet
        else:
            o = plan.orders[item.order_id]
            lsn += 1
            event = t.OrderChange(o.van_id, o.order_id, item.kind, o.stop_seq, o.dest_lat,
                                  o.dest_lon, o.promised_end, item.ts, lsn)  # fmt: skip
        state = states.setdefault(event.van_id, t.VanState())
        for o in t.step(state, event, arrival):
            if isinstance(o, t.Alert):
                out.append(o)
            elif isinstance(o, t.OrderEta) and o.eta is not None:
                late_by = o.eta - o.promised_end
                peaks[o.order_id] = max(peaks.get(o.order_id, late_by), late_by)
    PEAKS[id(out)] = peaks
    return out


def clean_stream(plan):
    start, end = plan.start - timedelta(hours=1), plan.end
    return merged_stream([plan], start, end)


@pytest.fixture(scope="module")
def day():
    """The demo day: 20 vans, 500 orders, 2 abnormal stops (`make demo`)."""
    plan = simulate_day(seed=7, local_date=DAY, vans=20, orders_per_day=500, abnormal_stops=2)
    return plan, replay(plan, clean_stream(plan))


def first_raises(alerts, kind):
    first: dict[str, t.Alert] = {}
    for a in alerts:
        if a.type == kind and a.event == "raised":
            first.setdefault(a.order_id or a.van_id, a)
    return first


def test_sla_risk_recall_and_precision_meet_the_targets(day):
    """AC5 (revised): recall >= 80 % over late orders that can be warned 15 min ahead,
    precision >= 70 %; recall over all late orders is reported."""
    plan, alerts = day
    first = first_raises(alerts, t.SLA_RISK)
    late = {o.order_id for o in plan.orders.values()
            if o.outcome == "delivered" and o.closed_at > o.promised_end}  # fmt: skip
    warnable = {
        oid for oid in late if plan.orders[oid].promised_end - plan.orders[oid].out_at >= LEAD
    }
    early = {oid for oid, a in first.items()
             if oid in late and plan.orders[oid].promised_end - a.raised_at >= LEAD}  # fmt: skip
    recall = len(early & warnable) / len(warnable)
    precision = len(first.keys() & late) / len(first)
    overall = len(early) / len(late)
    print(f"\nlate={len(late)} warnable={len(warnable)} alerted={len(first)} "
          f"recall={recall:.0%} precision={precision:.0%} all_late={overall:.0%}")  # fmt: skip
    assert recall >= 0.80
    assert precision >= 0.70
    assert 0 < overall <= recall


def test_each_injected_abnormal_stop_alerts_once_and_nothing_else_does(day):
    """AC6: one alert per injected stop, at idle time + at most one ping; none elsewhere."""
    plan, alerts = day
    raised = [a for a in alerts if a.type == t.ABNORMAL_STOP and a.event == "raised"]
    assert sorted(a.van_id for a in raised) == sorted(s.van_id for s in plan.abnormal_stops)
    for stop in plan.abnormal_stops:
        (alert,) = [a for a in raised if a.van_id == stop.van_id]
        assert stop.start <= alert.raised_at - t.IDLE_AFTER <= stop.start + timedelta(seconds=10)
        resolved = [a for a in alerts if a.alert_id == alert.alert_id and a.event == "resolved"]
        assert len(resolved) == 1


def test_a_clean_day_raises_no_abnormal_stop():
    plan = simulate_day(seed=11, local_date=DAY, vans=20, orders_per_day=500)
    assert [a for a in replay(plan, clean_stream(plan)) if a.type == t.ABNORMAL_STOP] == []


BORDERLINE = timedelta(minutes=2)


def test_chaos_gives_the_same_alerts(day):
    """AC8 (plan refinement 3, revised): the same alerts (type and subject). A raise moves later
    when chaos delayed the pings around it, by at most an offline burst (15 min), or a little
    earlier (at most 2 min) when the ETA it rests on shifted; see below. An SLA
    alert may appear in one run only if, in the other, the order came within 2 min of the
    threshold: pings flushed after an offline burst arrive after order changes that happened
    meanwhile, so their ETAs are computed on a shorter route."""
    plan, clean = day
    dirty = replay(plan, Chaos(CHAOS_DEFAULTS, seed=7).apply(clean_stream(plan)))
    peaks = {"clean": PEAKS[id(clean)], "dirty": PEAKS[id(dirty)]}
    for kind in (t.SLA_RISK, t.ABNORMAL_STOP):
        a, b = first_raises(clean, kind), first_raises(dirty, kind)
        for subject, other in [*((s, "dirty") for s in a.keys() - b.keys()),
                               *((s, "clean") for s in b.keys() - a.keys())]:  # fmt: skip
            assert kind == t.SLA_RISK, (kind, subject)
            assert peaks[other][subject] >= t.SLA_MARGIN - BORDERLINE, (subject, other)
        assert len(a.keys() ^ b.keys()) <= max(1, len(a) // 20)  # at most 5 % differ
        for subject in a.keys() & b.keys():
            shift = b[subject].raised_at - a[subject].raised_at
            assert -BORDERLINE <= shift <= timedelta(minutes=15), (kind, subject, shift)

"""The control tower rules, one van at a time (spec 006 AC2, AC3, AC4, AC6; BR-3)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from flink.udf import tower as t

T0 = datetime(2026, 11, 19, 3, 0, tzinfo=UTC)
VAN = "VAN-0001"
DEPOT = (10.80, 106.70)
NEAR_DEPOT = (10.8003, 106.7002)  # ~40 m
AWAY = (10.85, 106.70)  # ~5.5 km from the depot
STOP = (10.90, 106.70)  # ~11 km from the depot


def ping(seconds: float, at=DEPOT, speed=30.0) -> t.Ping:
    return t.Ping(VAN, T0 + timedelta(seconds=seconds), at[0], at[1], speed, DEPOT)


def order(oid="O1", status="out_for_delivery", end_min=60.0, lsn=1, stop=STOP, seq=1, at_s=0):
    end, at = T0 + timedelta(minutes=end_min), T0 + timedelta(seconds=at_s)
    return t.OrderChange(VAN, oid, status, seq, stop[0], stop[1], end, at, lsn)


def run(state, events, now=None):
    out = []
    for e in events:
        out += t.step(state, e, now or e.event_ts)
    return out


def alerts(outputs, kind=None):
    return [o for o in outputs if isinstance(o, t.Alert) and (kind is None or o.type == kind)]


def etas(outputs):
    return [o for o in outputs if isinstance(o, t.OrderEta)]


# ---- position (AC3) -------------------------------------------------------------------------
def test_latest_position_never_moves_back():
    s = t.VanState()
    assert run(s, [ping(10, AWAY)])
    assert run(s, [ping(5, DEPOT)]) == []  # older
    assert run(s, [ping(10, DEPOT)]) == []  # duplicate time
    assert s.latest.lat == AWAY[0]
    (latest,) = [o for o in run(s, [ping(15, DEPOT)]) if isinstance(o, t.VehicleLatest)]
    assert latest.event_ts == T0 + timedelta(seconds=15)


# ---- ETA (AC2) ------------------------------------------------------------------------------
def test_out_for_delivery_orders_get_an_eta_in_stop_order_and_assigned_ones_none():
    s = t.VanState()
    out = run(s, [
        order("O2", seq=2, stop=(10.95, 106.70), lsn=1, end_min=600),
        order("O1", seq=1, lsn=2, end_min=600),
        order("O3", status="assigned", lsn=3, end_min=600),
        ping(5, DEPOT, 30.0),
    ])  # fmt: skip
    last = {e.order_id: e for e in etas(out)}
    assert last["O3"].eta is None and last["O3"].status == "assigned"
    assert last["O1"].eta < last["O2"].eta  # O2 is the stop after O1
    assert last["O2"].distance_km > last["O1"].distance_km


def test_open_orders_are_listed_before_the_van_has_any_position():
    (row,) = etas(run(t.VanState(), [order(end_min=600)]))
    assert row.eta is None and row.order_id == "O1"


def test_an_older_or_duplicate_order_change_is_ignored():
    s = t.VanState()
    run(s, [order(lsn=5, end_min=600)])
    assert run(s, [order(status="assigned", lsn=4, end_min=600)]) == []
    assert s.orders["O1"].status == "out_for_delivery"


def test_a_closed_order_is_deleted_from_the_van():
    s = t.VanState()
    run(s, [order(end_min=600), ping(5)])
    (row,) = etas(run(s, [order(status="delivered", lsn=2, at_s=10)]))
    assert row.deleted and "O1" not in s.orders


# ---- SLA risk (AC4, BR-3) -------------------------------------------------------------------
def test_sla_risk_is_raised_once_while_open():
    s = t.VanState()
    out = run(s, [order(end_min=5)] + [ping(5 * i, DEPOT, 30.0) for i in range(1, 6)])
    (raised,) = alerts(out, t.SLA_RISK)
    assert raised.event == "raised" and raised.order_id == "O1"
    assert raised.alert_id == f"SLA_RISK:O1:{int(raised.raised_at.timestamp())}"


def test_sla_risk_resolves_on_delivery():
    s = t.VanState()
    run(s, [order(end_min=5), ping(5)])
    (resolved,) = alerts(run(s, [order(status="delivered", lsn=2, at_s=60)]))
    assert resolved.event == "resolved" and resolved.resolved_at == T0 + timedelta(seconds=60)


def test_sla_risk_resolves_when_the_eta_is_back_in_the_window_and_reraises_only_after_10_min():
    s = t.VanState()
    run(s, [order(end_min=24), ping(5, DEPOT, 30.0)])  # 11 km at 30 km/h = 22 min… then dwell 0
    assert s.sla["O1"].open is None  # 22 min < 24 min: on time
    late = run(s, [ping(10, DEPOT, 10.0)])  # jam: 11 km at the mean speed 20 km/h = 33 min
    assert alerts(late)[0].event == "raised"
    back = run(s, [ping(15, (10.85, 106.70), 60.0)])  # half way, fast
    assert alerts(back)[0].event == "resolved"
    again = run(s, [ping(20, DEPOT, 5.0)])  # late again, but resolved only 5 s ago
    assert alerts(again) == []
    later = run(s, [ping(15 + 601, DEPOT, 5.0)])  # resolved for more than 10 min
    assert alerts(later)[0].event == "raised"


def test_an_event_older_than_the_horizon_raises_nothing_but_updates_state():
    s = t.VanState()
    stale_now = T0 + timedelta(minutes=30)
    out = run(s, [order(end_min=5), ping(5)], now=stale_now)
    assert alerts(out) == [] and etas(out) and s.latest is not None
    fresh = run(s, [ping(25 * 60)], now=stale_now)  # 5 min old: inside the horizon
    assert alerts(fresh)[0].type == t.SLA_RISK


# ---- abnormal stop (AC6) --------------------------------------------------------------------
def still(start_s: float, minutes: float, at) -> list[t.Ping]:
    return [ping(start_s + i, at, 0.0) for i in range(0, int(minutes * 60) + 1, 5)]


def test_idle_10_min_away_from_stops_and_depot_raises_once_and_resolves_on_moving():
    s = t.VanState()
    out = run(s, still(0, 15, AWAY))
    (raised,) = alerts(out, t.ABNORMAL_STOP)
    assert raised.raised_at == T0 + timedelta(minutes=10)
    assert raised.alert_id == f"ABNORMAL_STOP:{VAN}:{int(T0.timestamp())}"
    (resolved,) = alerts(run(s, [ping(15 * 60 + 5, AWAY, 25.0)]))
    assert resolved.event == "resolved"


def test_idle_at_the_depot_or_at_a_stop_never_alerts():
    at_depot = run(t.VanState(), still(0, 60, NEAR_DEPOT))
    s = t.VanState()
    run(s, [order(end_min=600, stop=AWAY)])
    at_stop = run(s, still(5, 30, (AWAY[0] + 0.0005, AWAY[1])))  # ~55 m from the stop
    assert alerts(at_depot, t.ABNORMAL_STOP) == [] and alerts(at_stop, t.ABNORMAL_STOP) == []


def test_idle_shorter_than_10_min_never_alerts():
    s = t.VanState()
    out = run(s, still(0, 9.5, AWAY) + [ping(600, AWAY, 25.0)])
    assert alerts(out) == []


def test_a_gap_in_pings_or_a_new_place_starts_a_new_still_period():
    """Offline for 13 min between two still pings at different stops is not 13 min idle."""
    s = t.VanState()
    before = still(0, 3, AWAY)  # 3 min still, then offline
    after = still(16 * 60, 1, (10.86, 106.70))  # back online 16 min later, 1.1 km away, still
    assert alerts(run(s, before + after)) == []
    s = t.VanState()
    gap_same_place = still(0, 3, AWAY) + still(9 * 60, 2, AWAY)  # 6 min without pings
    assert alerts(run(s, gap_same_place)) == []

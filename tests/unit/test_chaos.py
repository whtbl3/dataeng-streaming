"""Spec 004 AC1: chaos mode sends the same day dirty, in exactly known amounts."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta

import pytest

from simulator.chaos import CHAOS_DEFAULTS, Chaos, ChaosConfig
from simulator.day import LOCAL_TZ, merged_stream, simulate_day

DAY = date(2026, 11, 18)
START = datetime(2026, 11, 18, 7, 30, tzinfo=LOCAL_TZ)
END = datetime(2026, 11, 18, 11, 30, tzinfo=LOCAL_TZ)


@pytest.fixture(scope="module")
def plan():
    return simulate_day(seed=42, local_date=DAY, vans=6, orders_per_day=120)


def _clean(plan):
    return list(merged_stream([plan], START, END))


def _dirty(plan, config=CHAOS_DEFAULTS, seed=42, until=None):
    chaos = Chaos(config, seed)
    return chaos, list(chaos.apply(iter(_clean(plan)), until=until))


def test_output_is_in_send_order_and_never_before_the_event(plan):
    _, out = _dirty(plan)
    send = [ts for ts, _, _ in out]
    assert send == sorted(send)
    for ts, kind, item in out:
        if kind == "ping":
            assert ts >= item.event_ts


def test_distinct_pings_equal_the_clean_run_and_orders_untouched(plan):
    clean = _clean(plan)
    _, out = _dirty(plan)
    assert {i for _, k, i in out if k == "ping"} == {i for _, k, i in clean if k == "ping"}
    assert [i for _, k, i in out if k == "order"] == [i for _, k, i in clean if k == "order"]
    assert [ts for ts, k, i in out if k == "order"] == [i.ts for _, k, i in clean if k == "order"]


def test_counts_are_exact(plan):
    chaos, out = _dirty(plan)
    pings = [i for _, k, i in out if k == "ping"]
    n_clean = sum(1 for _, k, _ in _clean(plan) if k == "ping")
    c = chaos.counts
    assert c["clean"] + c["duplicate"] + c["delayed"] + c["jitter"] + c["offline"] == n_clean
    assert len(pings) == n_clean + c["copies"]
    copies = Counter(pings)
    assert sum(n - 1 for n in copies.values()) == c["copies"]
    assert sum(1 for n in copies.values() if n > 1) == c["duplicate"]
    assert c["duplicate"] <= c["copies"] <= 3 * c["duplicate"]


def test_default_shares_are_close_to_the_configured_rates(plan):
    chaos, _ = _dirty(plan)
    c = chaos.counts
    n = c["clean"] + c["duplicate"] + c["delayed"] + c["jitter"]
    assert c["duplicate"] / n == pytest.approx(CHAOS_DEFAULTS.dup_rate, abs=0.01)
    assert c["delayed"] / n == pytest.approx(CHAOS_DEFAULTS.delay_rate, abs=0.01)
    assert c["jitter"] / n == pytest.approx(CHAOS_DEFAULTS.jitter_rate, abs=0.015)
    assert c["offline"] > 0


def test_delays_and_offline_flush(plan):
    chaos, out = _dirty(plan)
    lag = [(ts - i.event_ts) for ts, k, i in out if k == "ping"]
    assert max(lag) <= timedelta(minutes=CHAOS_DEFAULTS.max_delay_min)
    assert any(d > timedelta(minutes=10) for d in lag)  # the long tail exists
    # offline pings of one van arrive together after the window, in event order
    first_seen = {}
    for ts, k, i in out:
        if k == "ping":
            first_seen.setdefault(i, ts)
    for van, lo, hi in chaos.offline_windows:
        flushed = sorted(
            (p for p in first_seen if p.vehicle_id == van and lo <= p.event_ts < hi),
            key=lambda p: p.event_ts,
        )
        if not flushed:
            continue
        sends = [first_seen[p] for p in flushed]
        assert sends == sorted(sends)
        assert all(s >= hi for s in sends)


def test_same_seed_same_output_and_order_independent(plan):
    _, a = _dirty(plan)
    _, b = _dirty(plan)
    assert a == b
    # decisions depend on (van, event time), not on stream position
    clean = _clean(plan)
    later = [x for x in clean if x[0] >= START + timedelta(hours=1)]
    _, full = _dirty(plan)
    chaos = Chaos(CHAOS_DEFAULTS, 42)
    part = list(chaos.apply(iter(later)))
    cut = START + timedelta(hours=1)
    pings_full = Counter(i for _, k, i in full if k == "ping" and i.event_ts >= cut)
    pings_part = Counter(i for _, k, i in part if k == "ping")
    assert pings_full == pings_part


def test_off_means_identity(plan):
    clean = _clean(plan)
    chaos, out = _dirty(plan, ChaosConfig())
    assert out == clean
    assert chaos.counts["copies"] == 0


def test_until_clamps_sends_without_losing_pings(plan):
    clean = _clean(plan)
    until = clean[-1][0]
    _, out = _dirty(plan, until=until)
    assert max(ts for ts, _, _ in out) <= until
    assert {i for _, k, i in out if k == "ping"} == {i for _, k, i in clean if k == "ping"}


def test_invalid_config():
    with pytest.raises(ValueError):
        ChaosConfig(dup_rate=0.6, delay_rate=0.6)
    with pytest.raises(ValueError):
        ChaosConfig(delay_rate=0.1, max_delay_min=0)
    with pytest.raises(ValueError):
        ChaosConfig(jitter_rate=-0.1)

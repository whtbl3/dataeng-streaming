"""Labelled invalid-event injection (spec 003 AC7): tests know exactly what must reach the DLQ."""

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from contracts.decode import decode_gps
from simulator.fleet import Ping
from simulator.producer import KINDS, Corrupter, GpsEncoder

V2 = json.loads(Path("schemas/gps.pings.v1-value/v2.avsc").read_text())
NOW = datetime(2026, 11, 18, 5, 0, tzinfo=UTC)
PINGS = [
    Ping(
        f"VAN-{i % 20 + 1:04d}",
        datetime(2026, 11, 18, 4, 0, i % 60, tzinfo=UTC),
        10.78,
        106.69,
        30.0,
        90.0,
    )
    for i in range(4000)
]


class Reg:
    def schema_by_id(self, schema_id):
        return V2


def run(rate, seed=7):
    enc, bad = GpsEncoder(V2, 2), Corrupter(rate, seed, clock=lambda: NOW)
    out = [bad.encode(p, enc) for p in PINGS]
    return bad, out


def test_rate_zero_sends_only_valid_pings():
    bad, out = run(0.0)
    assert sum(bad.counts.values()) == 0
    assert all(decode_gps(v, NOW, Reg())[1] is None for _, v in out)


def test_about_the_given_share_is_invalid_spread_over_all_kinds():
    bad, _ = run(0.05)
    total = sum(bad.counts.values())
    assert 0.04 * len(PINGS) <= total <= 0.06 * len(PINGS)
    assert set(bad.counts) == set(KINDS)


def test_every_injected_ping_is_rejected_with_its_own_label():
    bad, out = run(0.05)
    seen = Counter()
    for _, value in out:
        _row, err = decode_gps(value, NOW, Reg())
        if err:
            seen[err[0]] += 1
    assert seen == bad.counts


def test_injection_is_deterministic_for_a_seed():
    assert run(0.05, seed=3)[1] == run(0.05, seed=3)[1]
    assert run(0.05, seed=3)[1] != run(0.05, seed=4)[1]


def test_keys_stay_the_real_vehicle_so_partitioning_is_unchanged():
    _, out = run(0.05)
    assert [k for k, _ in out] == [p.vehicle_id.encode() for p in PINGS]

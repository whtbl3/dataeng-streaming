"""Spec 004 AC7 and AC8: silver survives a hard stop and can be rebuilt from bronze (slow)."""

from __future__ import annotations

import subprocess
import time

import pytest
from conftest import ROOT, SILVER_GPS, TABLE, compose, count_table, query, run_gps_sim, wait_for

pytestmark = [pytest.mark.e2e, pytest.mark.restart]

TIMEOUT_S = 600


def _distinct(prefix: str) -> int:
    return query(
        f"SELECT count(DISTINCT (vehicle_id, event_ts)) FROM {TABLE} WHERE vehicle_id LIKE ?",
        (f"{prefix}-%",),
    )[0][0]


def _safe(fn):
    def call():
        try:
            return fn()
        except Exception:  # Trino or Flink still recovering
            return None

    return call


def test_taskmanager_kill_mid_load_keeps_silver_exact(prefix):
    """AC7: a hard stop while silver is loading loses nothing and duplicates nothing."""
    sent, chaos = run_gps_sim(prefix, "--chaos", vans=4, hours=2.0)
    wait_for(_safe(lambda: count_table(TABLE, prefix)), lambda n: n and n > 0, TIMEOUT_S)
    compose("kill", "taskmanager")
    compose("up", "-d", "taskmanager")
    wait_for(_safe(lambda: count_table(TABLE, prefix)), lambda n: n == sent, TIMEOUT_S)
    expected = _distinct(prefix)
    assert expected == sent - chaos.counts["copies"]
    wait_for(_safe(lambda: count_table(SILVER_GPS, prefix)), lambda n: n == expected, TIMEOUT_S)
    (rows, keys) = query(
        f"SELECT count(*), count(DISTINCT (vehicle_id, event_ts)) FROM {SILVER_GPS} "
        "WHERE vehicle_id LIKE ?",
        (f"{prefix}-%",),
    )[0]
    assert rows == keys == expected


CHECKSUMS = {
    "gps_pings": "checksum(ROW(vehicle_id, event_ts, lat, lon, speed_kmh, heading_deg, "
    "ingest_ts, lateness_s, is_late))",
    "orders": "checksum(ROW(order_id, created_at, depot_id, van_id, stop_seq, status, "
    "promised_end, delivered_at, failed_at, updated_at, last_lsn, last_commit_ts))",
    "order_status_history": "checksum(ROW(order_id, status, status_ts, source_lsn, commit_ts))",
}


def _caught_up() -> bool:
    """Silver holds every bronze key (bronze is quiet during this test)."""
    gps, orders = query(
        f"SELECT (SELECT count(DISTINCT (vehicle_id, event_ts)) FROM {TABLE}) "
        f"- (SELECT count(*) FROM {SILVER_GPS}), "
        "(SELECT count(DISTINCT after.order_id) FROM lakehouse.bronze.orders_cdc) "
        "- (SELECT count(*) FROM lakehouse.silver.orders)"
    )[0]
    return gps == 0 and orders == 0


def _silver_snapshot() -> dict[str, tuple]:
    return {
        t: tuple(query(f"SELECT count(*), {expr} FROM lakehouse.silver.{t}")[0])
        for t, expr in CHECKSUMS.items()
    }


def _stable_snapshot() -> dict[str, tuple]:
    """Wait until two snapshots one silver cycle apart are equal (nothing left to commit)."""
    last = None
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        snap = _safe(_silver_snapshot)()
        if snap is not None and snap == last:
            return snap
        last = snap
        time.sleep(45)
    pytest.fail(f"silver did not settle; last snapshot {last!r}")


def test_silver_rebuild_from_bronze_is_identical():
    """AC8: drop silver, rebuild it from bronze, and every row comes back identical."""
    wait_for(_safe(_caught_up), lambda ok: ok, TIMEOUT_S)
    before = _stable_snapshot()
    subprocess.run(["make", "-s", "silver-rebuild"], cwd=ROOT, check=True)
    after, _ = wait_for(_safe(_silver_snapshot), lambda s: s == before, TIMEOUT_S, 10)
    assert after == before

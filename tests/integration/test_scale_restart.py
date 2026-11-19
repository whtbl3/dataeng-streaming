"""Spec 005 AC4: a hard-killed Flink cluster resumes from its retained checkpoints (slow)."""

from __future__ import annotations

import subprocess

import pytest
from conftest import ROOT, SILVER_GPS, TABLE, compose, count_table, query, run_gps_sim, wait_for

pytestmark = [pytest.mark.e2e, pytest.mark.restart]

TIMEOUT_S = 600


def _safe(fn):
    def call():
        try:
            return fn()
        except Exception:  # Trino or Flink still recovering
            return None

    return call


def _exact(table: str, prefix: str) -> tuple[int, int]:
    """(rows, distinct keys) for one prefix."""
    return query(
        f"SELECT count(*), count(DISTINCT (vehicle_id, event_ts)) FROM {table} "
        "WHERE vehicle_id LIKE ?",
        (f"{prefix}-%",),
    )[0]


def test_killed_cluster_resumes_from_retained_checkpoints(prefix):
    """No savepoint (the JobManager is killed): every job resumes from its newest checkpoint, so
    bronze has each message once and silver each ping once, for pings sent before the kill and
    while Flink was down."""
    before, _ = run_gps_sim(f"{prefix}A", vans=3, hours=0.5)
    wait_for(_safe(lambda: count_table(TABLE, f"{prefix}A")), lambda n: n == before, TIMEOUT_S)
    compose("kill", "jobmanager", "taskmanager")
    during, _ = run_gps_sim(f"{prefix}B", vans=3, hours=0.5, seed=12)
    subprocess.run(["make", "-s", "up"], cwd=ROOT, check=True, capture_output=True, text=True)
    for p, sent in ((f"{prefix}A", before), (f"{prefix}B", during)):
        wait_for(_safe(lambda p=p: count_table(SILVER_GPS, p)), lambda n, s=sent: n == s, TIMEOUT_S)
        assert _exact(TABLE, p) == (sent, sent), p
        assert _exact(SILVER_GPS, p) == (sent, sent), p

"""Spec 005 AC4 and AC13: resuming after a hard kill, and after a stop longer than the snapshot
retention (slow, restarts jobs)."""

from __future__ import annotations

import os
import subprocess

import pytest
from conftest import (
    COMPOSE,
    ROOT,
    SILVER_GPS,
    TABLE,
    compose,
    count_table,
    query,
    run_gps_sim,
    wait_for,
)

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


@pytest.mark.parametrize("how", ["kill", "stop"])
def test_killed_cluster_resumes_from_retained_checkpoints(prefix, how):
    """No savepoint (the JobManager is killed, or stopped as on a reboot): every job resumes from
    its newest checkpoint, so bronze has each message once and silver each ping once, for pings
    sent before the stop and while Flink was down."""
    before, _ = run_gps_sim(f"{prefix}A", vans=3, hours=0.5)
    wait_for(_safe(lambda: count_table(TABLE, f"{prefix}A")), lambda n: n == before, TIMEOUT_S)
    compose(how, "jobmanager", "taskmanager")  # stop = SIGTERM, as on a reboot
    during, _ = run_gps_sim(f"{prefix}B", vans=3, hours=0.5, seed=12)
    subprocess.run(["make", "-s", "up"], cwd=ROOT, check=True, capture_output=True, text=True)
    for p, sent in ((f"{prefix}A", before), (f"{prefix}B", during)):
        wait_for(_safe(lambda p=p: count_table(SILVER_GPS, p)), lambda n, s=sent: n == s, TIMEOUT_S)
        assert _exact(TABLE, p) == (sent, sent), p
        assert _exact(SILVER_GPS, p) == (sent, sent), p


def _flink(method: str, path: str, body: dict | None = None) -> dict:
    import json
    import urllib.request

    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://localhost:8081{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def _stop_with_savepoint(job: str) -> None:
    """What `make down` does, for one job: stop it and leave the savepoint for submit.sh."""
    jid = next(
        j["jid"]
        for j in _flink("GET", "/jobs/overview")["jobs"]
        if j["name"] == job and j["state"] == "RUNNING"
    )
    target = f"file:///opt/flink/checkpoints/savepoints/{job}"
    trigger = _flink("POST", f"/jobs/{jid}/stop", {"targetDirectory": target, "drain": False})
    status, _ = wait_for(
        lambda: _flink("GET", f"/jobs/{jid}/savepoints/{trigger['request-id']}"),
        lambda r: r["status"]["id"] == "COMPLETED",
        120,
    )
    location = status["operation"]["location"]
    compose(
        "run", "--rm", "--no-deps", "--entrypoint", "bash", "flink-submit", "-c",
        f"echo {location} > /opt/flink/checkpoints/savepoints/{job}.pending",
    )  # fmt: skip


def test_reader_stopped_past_snapshot_retention_is_refused_and_rebuild_recovers(prefix):
    """AC13: silver_gps reads bronze by snapshot. Stopped longer than the snapshot retention, its
    position is gone: the submit refuses with the cause and the fix, instead of a job that loops
    or skips data; `make silver-rebuild` then gives exact silver."""
    _stop_with_savepoint("silver_gps")
    batches = []
    for i in range(3):  # three bronze commits while the reader is stopped
        p = f"{prefix}{i}"
        sent, _ = run_gps_sim(p, vans=2, hours=0.1, seed=20 + i)
        wait_for(_safe(lambda p=p: count_table(TABLE, p)), lambda n, s=sent: n == s, TIMEOUT_S)
        batches.append((p, sent))
    env = {"SNAPSHOT_RETENTION": "0s"}
    subprocess.run(
        ["make", "-s", "maintain", "TASK=expire", "TABLE=bronze.gps_pings"],
        cwd=ROOT, check=True, capture_output=True, text=True, env={**os.environ, **env},
    )  # fmt: skip

    out = subprocess.run(
        [*COMPOSE, "run", "--rm", "-e", "SNAPSHOT_RETENTION=0s", "flink-submit"],
        cwd=ROOT, capture_output=True, text=True,
    )  # fmt: skip
    assert out.returncode != 0
    assert "longer than the snapshot retention" in out.stderr + out.stdout
    assert "make silver-rebuild" in out.stderr + out.stdout
    assert "silver_gps" not in {
        j["name"] for j in _flink("GET", "/jobs/overview")["jobs"] if j["state"] == "RUNNING"
    }

    subprocess.run(
        ["make", "-s", "silver-rebuild"], cwd=ROOT, check=True, capture_output=True, text=True
    )
    for p, sent in batches:
        wait_for(_safe(lambda p=p: count_table(SILVER_GPS, p)), lambda n, s=sent: n == s, TIMEOUT_S)
        assert _exact(SILVER_GPS, p) == (sent, sent), p

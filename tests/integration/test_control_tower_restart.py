"""Spec 006 AC9: catch-up after a stop raises no stale alert and repeats none (slow, restarts).

The job is stopped with a savepoint (as `make down` does) while data keeps arriving. A stop of
30 minutes is simulated in event time: the pings sent meanwhile are 12-30 min old when the job
reads them, which is what a 30 min outage produces for the alert horizon (10 min).
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.request
from datetime import timedelta

import pytest
from conftest import COMPOSE, ROOT, wait_for
from test_control_tower import TIMEOUT_S, ReservedVan, alert_rows, now, pg

pytestmark = [pytest.mark.e2e, pytest.mark.restart]


def _flink(method: str, path: str, body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://localhost:8081{path}", data=data, method=method,
        headers={"Content-Type": "application/json"},
    )  # fmt: skip
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def _running(name: str) -> str | None:
    jobs = _flink("GET", "/jobs/overview")["jobs"]
    return next((j["jid"] for j in jobs if j["name"] == name and j["state"] == "RUNNING"), None)


def _stop_with_savepoint(name: str) -> None:
    jid = _running(name)
    target = f"file:///opt/flink/checkpoints/savepoints/{name}"
    trigger = _flink("POST", f"/jobs/{jid}/stop", {"targetDirectory": target, "drain": False})
    status, _ = wait_for(
        lambda: _flink("GET", f"/jobs/{jid}/savepoints/{trigger['request-id']}"),
        lambda r: r["status"]["id"] == "COMPLETED",
        120,
    )
    location = status["operation"]["location"]
    subprocess.run(
        [*COMPOSE, "run", "--rm", "--no-deps", "--entrypoint", "bash", "flink-submit", "-c",
         f"echo {location} > /opt/flink/checkpoints/savepoints/{name}.pending"],
        cwd=ROOT, check=True, capture_output=True, text=True,
    )  # fmt: skip


def test_catch_up_raises_no_stale_alert_and_none_twice():
    t0 = now() - timedelta(minutes=30)
    before, idle, after = ReservedVan(), ReservedVan(pings_from=t0), ReservedVan()
    try:
        # an alert raised before the stop
        before.order(north_km=8, promised_in_min=5)
        time.sleep(3)
        before.ping(before.place(0), 30.0)
        (raised,), _ = wait_for(
            lambda: alert_rows(before.id, "SLA_RISK", since=before.started),
            lambda r: len(r) == 1,
            TIMEOUT_S,
        )

        _stop_with_savepoint("control_tower")
        assert _running("control_tower") is None
        # while stopped: 18 min idle away from everything, 30 to 12 min ago, then driving
        spot = idle.place(3)
        for i in range(0, 18 * 60 + 1, 5):
            idle.ping(spot, 0.0, ts=t0 + timedelta(seconds=i))
        last = idle.ping(idle.place(3.5), 25.0, ts=t0 + timedelta(minutes=18, seconds=30))
        before.ping(before.place(0.1), 30.0)  # the at-risk order is still at risk

        subprocess.run(["make", "-s", "up"], cwd=ROOT, check=True, capture_output=True, text=True)
        wait_for(lambda: _running("control_tower"), lambda j: j is not None, TIMEOUT_S)

        # positions catch up, but the stale idle period raises nothing
        wait_for(
            lambda: pg("SELECT event_ts FROM serving.vehicle_latest WHERE van_id = %s", (idle.id,)),
            lambda r: r and r[0][0] == last,
            TIMEOUT_S,
        )
        assert alert_rows(idle.id, "ABNORMAL_STOP", since=idle.started) == []
        # the alert from before the stop is not raised again (state came back with the savepoint)
        assert alert_rows(before.id, "SLA_RISK", since=before.started) == [raised]
        # and the alert path is current: a fresh risk alerts at once (NFR-4: within 15 min)
        after.order(north_km=8, promised_in_min=5)
        time.sleep(3)
        after.ping(after.place(0), 30.0)
        wait_for(
            lambda: alert_rows(after.id, "SLA_RISK", since=after.started),
            lambda r: len(r) == 1,
            TIMEOUT_S,
        )
    finally:
        for v in (before, idle, after):
            v.teardown()

"""Spec 002 acceptance tests: orders CDC against the running stack (`make up`, `make e2e`)."""

from __future__ import annotations

import subprocess
import time

import pytest
from conftest import (
    BOOTSTRAP,
    ROOT,
    compose,
    compose_services,
    connector_running,
    pg_query,
    query,
    run_orders_sim,
    wait_for,
)

from simulator.world import DEPOTS, VANS

pytestmark = pytest.mark.e2e

ORDERS = "lakehouse.bronze.orders_cdc"
CHECKPOINT_S = 30
NEXT_STATUS = {
    None: {"created"},
    "created": {"assigned"},
    "assigned": {"out_for_delivery"},
    "out_for_delivery": {"delivered", "failed"},
}


def _order_rows(prefix: str) -> int:
    return query(
        f"SELECT count(*) FROM {ORDERS} WHERE coalesce(after.order_id, before.order_id) LIKE ?",
        (f"{prefix}-%",),
    )[0][0]


def test_cdc_stack_healthy_and_seeded():
    """AC1: order DB, Connect and connector healthy; CDC topics exist; seed = world.py."""
    services = {s["Service"]: s for s in compose_services()}
    for name in ("postgres", "connect"):
        assert services[name]["State"] == "running", name
        assert services[name].get("Health", "") in ("", "healthy"), name
    assert services["connect-init"]["State"] == "exited"
    assert services["connect-init"]["ExitCode"] == 0
    assert connector_running()

    from confluent_kafka.admin import AdminClient

    topics = AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=10).topics
    for t in ("cdc.public.depots", "cdc.public.vans", "cdc.public.orders"):
        assert t in topics

    assert [r[0] for r in pg_query("SELECT depot_id FROM depots ORDER BY 1")] == [
        d.depot_id for d in DEPOTS
    ]
    assert [r[0] for r in pg_query("SELECT van_id FROM vans ORDER BY 1")] == [
        v.van_id for v in VANS
    ]


def test_seed_captured_once_as_snapshot():
    """AC6: rows that existed before the connector started are captured once, as reads."""

    def snapshot_counts() -> tuple[int | None, int | None]:
        depots = dict(query("SELECT op, count(*) FROM lakehouse.bronze.depots_cdc GROUP BY op"))
        vans = dict(query("SELECT op, count(*) FROM lakehouse.bronze.vans_cdc GROUP BY op"))
        return depots.get("r"), vans.get("r")

    # right after a cold `make up` the snapshot lands on the next Iceberg commit (checkpoint)
    wait_for(snapshot_counts, lambda c: c[0] is not None and c[1] is not None, timeout_s=90)
    time.sleep(CHECKPOINT_S + 5)  # one more checkpoint: a second snapshot must not appear
    assert snapshot_counts() == (len(DEPOTS), len(VANS))


def test_n_changes_n_rows_in_commit_order(prefix):
    """AC5: N order changes -> exactly N bronze rows, with CDC metadata, in commit order."""
    n = run_orders_sim(prefix)
    assert n > 0
    wait_for(lambda: _order_rows(prefix), lambda c: c >= n, timeout_s=120)
    time.sleep(CHECKPOINT_S + 5)  # nothing extra may appear on the next checkpoint
    rows = query(
        f"SELECT after.order_id, op, before.status, after.status, source_lsn, commit_ts "
        f"FROM {ORDERS} WHERE after.order_id LIKE ? ORDER BY after.order_id, source_lsn",
        (f"{prefix}-%",),
    )
    assert len(rows) == n
    assert len({r[4] for r in rows}) == n  # one commit per change
    last_status: dict[str, str | None] = {}
    for order_id, op, before_status, after_status, lsn, commit_ts in rows:
        assert lsn is not None and commit_ts is not None
        prev = last_status.get(order_id)
        assert after_status in NEXT_STATUS[prev], (order_id, prev, after_status)
        if op == "c":
            assert prev is None and before_status is None
        else:
            assert op == "u" and before_status == prev  # full before-image
        last_status[order_id] = after_status


def test_connector_restart_loses_and_duplicates_nothing(prefix):
    """AC7: stop the connector, write M changes, start it again -> exactly M rows."""
    compose("stop", "connect")
    try:
        m = run_orders_sim(prefix)
        assert m > 0
    finally:
        compose("start", "connect")
    wait_for(connector_running, lambda ok: ok, timeout_s=180)
    wait_for(lambda: _order_rows(prefix), lambda c: c >= m, timeout_s=180)
    time.sleep(CHECKPOINT_S + 5)
    rows = query(
        f"SELECT count(*), count(DISTINCT source_lsn) FROM {ORDERS} WHERE after.order_id LIKE ?",
        (f"{prefix}-%",),
    )
    assert rows[0] == (m, m)


def test_cdc_status_shows_state_and_lag():
    """AC8 (first half): one command shows connector state and replication slot lag."""
    out = subprocess.run(
        ["make", "-s", "cdc-status"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout
    assert '"state":"RUNNING"' in out
    assert "ct_cdc_slot" in out
    assert "lag" in out

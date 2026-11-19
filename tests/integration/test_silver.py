"""Spec 004 acceptance tests: silver against the running stack (`make up`, `make e2e`)."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import pytest
from conftest import (
    SILVER_GPS,
    TABLE,
    count_table,
    query,
    run_gps_sim,
    run_orders_sim,
    send_ping,
    silver_gps_rows,
    wait_for,
)

from simulator.day import LOCAL_TZ
from simulator.fleet import Ping

pytestmark = pytest.mark.e2e

# bronze commit (30 s) + silver poll (10 s) + silver commit (30 s), with margin
SILVER_TIMEOUT_S = 240
ORDERS_CDC = "lakehouse.bronze.orders_cdc"
SILVER_ORDERS = "lakehouse.silver.orders"
HISTORY = "lakehouse.silver.order_status_history"
LIFECYCLES = (
    ["created", "assigned", "out_for_delivery", "delivered"],
    ["created", "assigned", "out_for_delivery", "failed"],
)
# Bronze changes of the test's orders, each with its order's highest commit position (x.max_lsn).
_WITH_MAX_LSN = (
    f"FROM {ORDERS_CDC} c JOIN (SELECT after.order_id AS oid, max(source_lsn) AS max_lsn "
    f"FROM {ORDERS_CDC} WHERE after.order_id LIKE ? GROUP BY 1) x ON c.after.order_id = x.oid"
)


def _distinct_bronze(prefix: str) -> int:
    return query(
        f"SELECT count(DISTINCT (vehicle_id, event_ts)) FROM {TABLE} WHERE vehicle_id LIKE ?",
        (f"{prefix}-%",),
    )[0][0]


def _wait_silver(prefix: str, expected: int) -> None:
    wait_for(lambda: count_table(SILVER_GPS, prefix), lambda n: n >= expected, SILVER_TIMEOUT_S)


def _ping(prefix: str, ts: datetime, van: int = 1) -> Ping:
    return Ping(f"{prefix}-{van:04d}", ts, 10.7769, 106.7009, 20.0, 90.0)


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def test_clean_and_chaos_runs_give_identical_silver(prefix):
    """AC2: the same day, sent clean and dirty, gives the same silver rows."""
    clean, dirty = f"{prefix}C", f"{prefix}X"
    n_clean, _ = run_gps_sim(clean, hours=1.0)
    n_dirty, chaos = run_gps_sim(dirty, "--chaos", hours=1.0)
    assert chaos.counts["copies"] > 0 and chaos.counts["delayed"] > 0
    assert n_dirty == n_clean + chaos.counts["copies"]
    wait_for(lambda: count_table(TABLE, dirty), lambda n: n == n_dirty, SILVER_TIMEOUT_S)
    _wait_silver(clean, n_clean)
    _wait_silver(dirty, n_clean)
    assert count_table(TABLE, dirty) > count_table(TABLE, clean)
    event_fields = [r[:6] for r in silver_gps_rows(clean)]
    assert event_fields == [r[:6] for r in silver_gps_rows(dirty)]
    assert len(event_fields) == n_clean
    late = [r for r in silver_gps_rows(dirty) if r[8]]
    assert late, "chaos delays should leave late rows in silver"


def test_two_hour_late_ping_is_kept_and_marked_late(prefix):
    """AC3: a ping 2 h behind the watermark is stored, marked late, with its lateness."""
    now = _now()
    send_ping(_ping(prefix, now - timedelta(seconds=5)), sent_at=now - timedelta(seconds=4))
    send_ping(_ping(prefix, now - timedelta(hours=2)), sent_at=now)
    _wait_silver(prefix, 2)
    rows = {r[1]: r for r in silver_gps_rows(prefix)}
    late = rows[now - timedelta(hours=2)]
    fresh = rows[now - timedelta(seconds=5)]
    assert late[8] is True and late[7] == 7200
    assert fresh[8] is False and fresh[7] == 1


def test_resending_pings_changes_no_silver_row(prefix):
    """AC4: copies that arrive later (here: minutes after the first) change nothing."""
    now = _now()
    pings = [_ping(prefix, now - timedelta(minutes=10, seconds=5 * i)) for i in range(5)]
    for p in pings:
        send_ping(p, sent_at=p.event_ts)
    _wait_silver(prefix, 5)
    before = silver_gps_rows(prefix)
    for p in pings:
        send_ping(p, sent_at=_now())
    marker = _ping(prefix, now)  # same van, so same partition, sent after the copies
    send_ping(marker, sent_at=_now())
    _wait_silver(prefix, 6)  # the marker is in silver, so the copies were processed too
    assert count_table(TABLE, prefix) == 11
    after = [r for r in silver_gps_rows(prefix) if r[1] != now]
    assert after == before


def test_live_ping_reaches_silver_within_2_min(prefix):
    """AC9: a ping sent now is queryable in silver within 2 minutes."""
    now = _now()
    send_ping(_ping(prefix, now), sent_at=now)
    start = time.monotonic()
    _wait_silver(prefix, 1)
    assert time.monotonic() - start < 120


@pytest.fixture(scope="module")
def orders_prefix() -> str:
    """One orders simulation shared by AC5 and AC6, with dirty copies injected into bronze."""
    import uuid

    pre = f"S4O{uuid.uuid4().hex[:6]}"
    changes = run_orders_sim(pre, vans=3, orders_per_day=12, hours=24)
    like = (f"{pre}-%",)
    wait_for(
        lambda: query(f"SELECT count(*) FROM {ORDERS_CDC} WHERE after.order_id LIKE ?", like)[0][0],
        lambda n: n >= changes,
        SILVER_TIMEOUT_S,
    )
    # Dirty input, as a Debezium restart or a replay would leave it: every change again (a
    # duplicate), and then the older versions once more, so they arrive after the newest one.
    query(f"INSERT INTO {ORDERS_CDC} SELECT * FROM {ORDERS_CDC} WHERE after.order_id LIKE ?", like)
    query(
        f"INSERT INTO {ORDERS_CDC} SELECT c.* {_WITH_MAX_LSN} WHERE c.source_lsn < x.max_lsn",
        like,
    )
    return pre


def _latest_versions(pre: str) -> list[tuple]:
    return query(
        "SELECT c.after.order_id, c.after.status, c.after.van_id, c.after.delivered_at, "
        f"c.source_lsn {_WITH_MAX_LSN} WHERE c.source_lsn = x.max_lsn "
        "GROUP BY 1, 2, 3, 4, 5 ORDER BY 1",
        (f"{pre}-%",),
    )


def test_order_latest_version_wins_despite_duplicates_and_disorder(orders_prefix):
    """AC5: one row per order, equal to the change with the highest commit position."""
    expected = _latest_versions(orders_prefix)
    assert expected

    def silver():
        return query(
            f"SELECT order_id, status, van_id, delivered_at, last_lsn FROM {SILVER_ORDERS} "
            "WHERE order_id LIKE ? ORDER BY 1",
            (f"{orders_prefix}-%",),
        )

    rows, _ = wait_for(silver, lambda r: r == expected, SILVER_TIMEOUT_S)
    assert len({r[0] for r in rows}) == len(rows)
    time.sleep(90)  # the injected copies are in a later bronze snapshot: give it a silver cycle
    assert silver() == expected


def test_status_history_one_row_per_status_in_commit_order(orders_prefix):
    """AC6: created → assigned → out for delivery → delivered (or failed), in commit order."""
    n_orders = len(_latest_versions(orders_prefix))
    (n_points,) = query(
        "SELECT count(created_at) + count(assigned_at) + count(out_for_delivery_at) "
        "+ count(delivered_at) + count(failed_at) FROM ("
        "SELECT c.after.created_at, c.after.assigned_at, c.after.out_for_delivery_at, "
        f"c.after.delivered_at, c.after.failed_at {_WITH_MAX_LSN} "
        "WHERE c.source_lsn = x.max_lsn GROUP BY 1, 2, 3, 4, 5)",
        (f"{orders_prefix}-%",),
    )[0]

    def history():
        return query(
            f"SELECT order_id, status, status_ts, source_lsn FROM {HISTORY} "
            "WHERE order_id LIKE ? ORDER BY order_id, source_lsn, status_ts",
            (f"{orders_prefix}-%",),
        )

    rows, _ = wait_for(history, lambda r: len(r) == n_points, SILVER_TIMEOUT_S)
    by_order: dict[str, list[tuple]] = {}
    for order_id, status, ts, lsn in rows:
        by_order.setdefault(order_id, []).append((status, ts, lsn))
    assert len(by_order) == n_orders
    for order_id, steps in by_order.items():
        statuses = [s for s, _, _ in steps]
        # a prefix of a valid lifecycle: open orders have not reached the end yet
        assert any(statuses == life[: len(statuses)] for life in LIFECYCLES), (order_id, statuses)
        times = [t for _, t, _ in steps]
        assert times == sorted(times), order_id
    # status_ts is the business time recorded by the order system, not the commit time
    sample = query(
        f"SELECT h.status_ts = o.delivered_at FROM {HISTORY} h JOIN {SILVER_ORDERS} o "
        "ON h.order_id = o.order_id WHERE h.status = 'delivered' AND h.order_id LIKE ?",
        (f"{orders_prefix}-%",),
    )
    assert sample and all(ok for (ok,) in sample)


def _status_rows() -> dict[tuple[str, str], list[str]]:
    import subprocess

    from conftest import ROOT

    out = subprocess.run(
        ["make", "-s", "silver-status"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout
    lines = [ln for ln in out.splitlines() if "|" in ln]
    header = [c.strip() for c in lines[0].split("|")]
    assert header == [
        "silver_table",
        "local_day",
        "bronze_rows",
        "silver_rows",
        "removed",
        "late_rows",
        "conflicting",
    ]
    rows = [[c.strip() for c in ln.split("|")] for ln in lines[1:]]
    return {(r[0], r[1]): r[2:] for r in rows}


def test_silver_status_reports_duplicates_and_late_rows(prefix):
    """AC10: per table and day, bronze rows, silver rows, removed copies and late rows."""
    now = _now()
    ping = _ping(prefix, now - timedelta(minutes=3))
    send_ping(ping, sent_at=ping.event_ts + timedelta(minutes=2))  # late
    send_ping(ping, sent_at=_now())  # a copy
    _wait_silver(prefix, 1)
    day = ping.event_ts.astimezone(LOCAL_TZ).date().isoformat()
    lo = datetime.fromisoformat(day).replace(tzinfo=LOCAL_TZ)
    hi = lo + timedelta(days=1)

    def direct():
        return query(
            f"SELECT (SELECT count(*) FROM {TABLE} WHERE event_ts >= ? AND event_ts < ?), "
            f"(SELECT count(*) FROM {SILVER_GPS} WHERE event_ts >= ? AND event_ts < ?), "
            f"(SELECT count_if(is_late) FROM {SILVER_GPS} WHERE event_ts >= ? AND event_ts < ?)",
            (lo, hi, lo, hi, lo, hi),
        )[0]

    def both():  # jobs keep committing, so read both sides until they agree
        bronze, silver, late = direct()
        status = [int(x) for x in _status_rows()[("gps_pings", day)][:4]]
        return status, [bronze, silver, bronze - silver, late]

    (_, (bronze, silver, _, late)), _ = wait_for(both, lambda t: t[0] == t[1], 120)
    assert bronze - silver >= 1 and late >= 1

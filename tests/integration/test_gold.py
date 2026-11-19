"""Spec 007 acceptance tests: control tower history in bronze and gold built by dbt (`make up`).

The dbt scheduler service is stopped while these tests run dbt themselves (no racing builds).
"""

from __future__ import annotations

import subprocess
import time
import uuid
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from conftest import COMPOSE, ROOT, compose, pg_query, query, run_orders_sim, wait_for
from test_control_tower import ReservedVan, pg
from test_silver import _WITH_MAX_LSN, ORDERS_CDC

pytestmark = pytest.mark.e2e

BRONZE_TIMEOUT_S = 150  # control tower checkpoint (30 s) commits the Iceberg sinks


def test_alert_events_and_eta_marks_reach_bronze():
    """AC1: every alert raise/resolve and the T30/T15 ETA marks land in bronze, once each."""
    van = ReservedVan()
    try:
        # deadline in 30 min + 20 s: the first ping is before T-30, the second after it
        oid = van.order(north_km=12, promised_in_min=30 + 20 / 60)
        time.sleep(3)
        van.ping(van.place(0), 30.0)
        time.sleep(25)
        van.ping(van.place(0.1), 30.0)  # 12 km at 30 km/h = 24 min: on time, no alert yet
        marks, _ = wait_for(
            lambda: query(
                "SELECT mark, eta, promised_end FROM lakehouse.bronze.eta_marks WHERE order_id = ?",
                (oid,),
            ),
            lambda rows: len(rows) == 1,
            BRONZE_TIMEOUT_S,
        )
        assert marks[0][0] == "T30"

        late = van.order(north_km=12, promised_in_min=5)  # 24 min away: at risk
        time.sleep(3)
        van.ping(van.place(0.2), 30.0)
        time.sleep(3)
        van.close(late)
        events, _ = wait_for(
            lambda: query(
                "SELECT event, type, alert_id FROM lakehouse.bronze.alert_events "
                "WHERE order_id = ? ORDER BY event",
                (late,),
            ),
            lambda rows: [r[0] for r in rows] == ["raised", "resolved"],
            BRONZE_TIMEOUT_S,
        )
        assert {r[1] for r in events} == {"SLA_RISK"} and len({r[2] for r in events}) == 1
    finally:
        van.teardown()


# ---- gold built by dbt (AC2, AC4, AC7-AC12) -------------------------------------------------
GOLD_TIMEOUT_S = 300
FACT = "lakehouse.gold.fct_delivery"


@pytest.fixture(scope="module", autouse=True)
def no_scheduled_gold_runs():
    """The tests run dbt themselves; the scheduler's own runs would race with them."""
    compose("stop", "dbt", check=False)
    yield
    compose("up", "-d", "dbt", check=False)


def make(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["make", "-s", *args], cwd=ROOT, check=check, capture_output=True, text=True
    )


def closed_in_db(prefix: str) -> int:
    return pg_query(
        "SELECT count(*) FROM orders WHERE order_id LIKE %s AND status IN ('delivered','failed')",
        (f"{prefix}-%",),
    )[0][0]


def closed_in_silver(prefix: str) -> int:
    return query(
        "SELECT count(*) FROM lakehouse.silver.orders WHERE order_id LIKE ? "
        "AND status IN ('delivered', 'failed')",
        (f"{prefix}-%",),
    )[0][0]


def simulate(prefix: str, seed_vans: int = 3) -> int:
    run_orders_sim(prefix, vans=seed_vans, orders_per_day=12, hours=24)
    want = closed_in_db(prefix)
    wait_for(lambda: closed_in_silver(prefix), lambda n: n == want, GOLD_TIMEOUT_S)
    return want


def facts(prefix: str) -> list[tuple]:
    """Delivery KPIs per order, the prefix removed so two runs of one seed compare."""
    return query(
        "SELECT substr(order_id, length(?) + 1), local_date, outcome, is_on_time, "
        "minutes_late, promised_end, closed_at, is_final FROM " + FACT + " WHERE order_id LIKE ? "
        "ORDER BY 1",
        (prefix, f"{prefix}-%"),
    )


def test_one_fact_per_closed_order_and_no_exact_destination():
    """AC2 and AC3 on the real tables (the dbt tests also run in every build)."""
    prefix = f"G2{uuid.uuid4().hex[:5].upper()}"
    simulate(prefix)
    make("gold")
    # closed days are not touched by the intraday build (AC9), so count the open days only
    (n,) = query(
        "SELECT count(*) FROM lakehouse.gold_staging.stg_orders_closed WHERE order_id LIKE ? "
        "AND local_date NOT IN (SELECT local_date FROM lakehouse.gold.closed_days)",
        (f"{prefix}-%",),
    )[0]
    rows = facts(prefix)
    assert n > 0 and len(rows) == n == len({r[0] for r in rows})
    assert all(not r[7] for r in rows)  # provisional until the close
    columns = {c for (c,) in query("SELECT column_name FROM lakehouse.information_schema.columns "
                                   "WHERE table_schema = 'gold'")}  # fmt: skip
    assert "dest_area" in columns and not columns & {"dest_lat", "dest_lon"}


def test_a_van_that_moves_depot_keeps_its_history():
    """AC4: deliveries before the move stay with the old depot, later ones go to the new one."""
    van = pg("SELECT van_id, depot_id FROM vans WHERE van_id = 'VAN-0150'")[0]
    other = "DEP-02" if van[1] != "DEP-02" else "DEP-03"
    tag = uuid.uuid4().hex[:6].upper()
    t = datetime.now(UTC).replace(microsecond=0)

    def closed_order(oid: str, at: datetime) -> None:
        pg(
            "INSERT INTO orders (order_id, depot_id, van_id, stop_seq, dest_lat, dest_lon, "
            "promised_start, promised_end, status, created_at, assigned_at, out_for_delivery_at, "
            "delivered_at, updated_at) VALUES (%s, %s, %s, 1, 10.78, 106.70, %s, %s, 'delivered', "
            "%s, %s, %s, %s, %s)",
            (oid, van[1], van[0], at - timedelta(hours=2), at + timedelta(hours=1),
             at - timedelta(hours=3), at - timedelta(hours=2), at - timedelta(hours=1), at, at),
        )  # fmt: skip

    try:
        closed_order(f"G4{tag}-1", t - timedelta(minutes=30))
        time.sleep(1)
        pg("UPDATE vans SET depot_id = %s, updated_at = now() WHERE van_id = %s", (other, van[0]))
        time.sleep(1)
        closed_order(f"G4{tag}-2", datetime.now(UTC).replace(microsecond=0))
        wait_for(lambda: closed_in_silver(f"G4{tag}"), lambda n: n == 2, GOLD_TIMEOUT_S)
        wait_for(
            lambda: query("SELECT count(*) FROM lakehouse.bronze.vans_cdc "
                          "WHERE after.van_id = ? AND after.depot_id = ?",
                          (van[0], other))[0][0],
            lambda n: n >= 1,
            GOLD_TIMEOUT_S,
        )  # fmt: skip
        make("gold")
        rows = dict(query(f"SELECT order_id, depot_id FROM {FACT} WHERE order_id LIKE ?",
                          (f"G4{tag}-%",)))  # fmt: skip
        assert rows == {f"G4{tag}-1": van[1], f"G4{tag}-2": other}
    finally:
        pg("UPDATE vans SET depot_id = %s, updated_at = now() WHERE van_id = %s", (van[1], van[0]))


def _yesterday_local() -> str:
    return (datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date() - timedelta(days=1)).isoformat()


def _snapshot_added_records() -> int:
    (n,) = query(
        "SELECT CAST(element_at(summary, 'added-records') AS bigint) "
        'FROM lakehouse.gold."fct_delivery$snapshots" ORDER BY committed_at DESC LIMIT 1'
    )[0]
    return n or 0


def test_intraday_touches_only_recent_changes_and_close_is_final_and_idempotent():
    """AC7 and AC8."""
    prefix = f"G7{uuid.uuid4().hex[:5].upper()}"
    simulate(prefix)
    make("gold")
    make("gold")  # nothing changed: the second run rewrites at most the 2 h window
    (total,) = query(f"SELECT count(*) FROM {FACT}")[0]
    assert _snapshot_added_records() < total

    day = _yesterday_local()
    make("close", f"DAY={day}")
    first = query(f"SELECT order_id, is_final, closed_at FROM {FACT} "
                  f"WHERE local_date = DATE '{day}' ORDER BY 1")  # fmt: skip
    closed = query(f"SELECT closed_at, restatements FROM lakehouse.gold.closed_days "
                   f"WHERE local_date = DATE '{day}'")  # fmt: skip
    assert first and all(r[1] for r in first) and len(closed) == 1
    make("close", f"DAY={day}")  # again: nothing changes
    assert query(f"SELECT order_id, is_final, closed_at FROM {FACT} "
                 f"WHERE local_date = DATE '{day}' ORDER BY 1") == first  # fmt: skip
    assert query(f"SELECT closed_at, restatements FROM lakehouse.gold.closed_days "
                 f"WHERE local_date = DATE '{day}'") == closed  # fmt: skip


def test_a_late_change_after_the_close_is_counted_then_restated():
    """AC9: the closed day does not change until an explicit, logged restatement."""
    prefix = f"G9{uuid.uuid4().hex[:5].upper()}"
    simulate(prefix)
    day = _yesterday_local()
    make("gold")
    # (re)close the day now, after this test's orders arrived (an earlier test may have closed it)
    make("reclose", f"DAY={day}")
    (oid, closed_at) = query(f"SELECT order_id, closed_at FROM {FACT} WHERE order_id LIKE ? "
                             f"AND local_date = DATE '{day}' AND outcome = 'delivered' LIMIT 1",
                             (f"{prefix}-%",))[0]  # fmt: skip
    before = query(f"SELECT restatements, late_after_close FROM lakehouse.gold.closed_days "
                   f"WHERE local_date = DATE '{day}'")[0]  # fmt: skip
    later = closed_at + timedelta(minutes=1)  # same local day: simulated orders close by day
    pg("UPDATE orders SET delivered_at = %s, updated_at = now() WHERE order_id = %s", (later, oid))
    wait_for(
        lambda: query("SELECT delivered_at FROM lakehouse.silver.orders WHERE order_id = ?",
                      (oid,))[0][0],
        lambda v: v == later,
        GOLD_TIMEOUT_S,
    )  # fmt: skip
    make("gold")
    assert query(f"SELECT closed_at FROM {FACT} WHERE order_id = ?", (oid,))[0][0] == closed_at
    after = query(f"SELECT restatements, late_after_close FROM lakehouse.gold.closed_days "
                  f"WHERE local_date = DATE '{day}'")[0]  # fmt: skip
    assert after[1] >= before[1] + 1
    make("reclose", f"DAY={day}")
    assert query(f"SELECT closed_at, is_final FROM {FACT} WHERE order_id = ?", (oid,))[0] == (
        later, True)  # fmt: skip
    assert query(f"SELECT restatements FROM lakehouse.gold.closed_days "
                 f"WHERE local_date = DATE '{day}'")[0][0] == before[0] + 1  # fmt: skip


def test_dirty_cdc_gives_the_same_gold_kpis_as_clean():
    """AC10 (plan refinement 2): the same day, once clean and once with duplicate and
    out-of-order changes in bronze, gives identical delivery KPIs, provisional and final."""
    clean, dirty = (f"G10{x}{uuid.uuid4().hex[:4].upper()}" for x in "CD")
    simulate(clean)
    simulate(dirty)
    like = (f"{dirty}-%",)
    query(f"INSERT INTO {ORDERS_CDC} SELECT * FROM {ORDERS_CDC} WHERE after.order_id LIKE ?", like)
    query(f"INSERT INTO {ORDERS_CDC} SELECT c.* {_WITH_MAX_LSN} WHERE c.source_lsn < x.max_lsn",
          like)  # fmt: skip
    time.sleep(70)  # the silver job reads the injected copies (two commits)
    make("gold")
    assert facts(clean) == facts(dirty) and facts(clean)
    make("close", f"DAY={_yesterday_local()}")
    assert facts(clean) == facts(dirty)


def test_a_broken_model_fails_the_data_tests():
    """AC11: a duplicate fact (as a broken merge would leave) fails `make gold-test`."""
    (oid,) = query(f"SELECT order_id FROM {FACT} LIMIT 1")[0]
    keep = f"lakehouse.gold.e2e_keep_{uuid.uuid4().hex[:6]}"
    query(f"CREATE TABLE {keep} AS SELECT * FROM {FACT} WHERE order_id = ?", (oid,))
    query(f"INSERT INTO {FACT} SELECT * FROM {keep}")
    try:
        failed = make("gold-test", check=False)
        assert failed.returncode != 0 and "unique_fct_delivery_order_id" in failed.stdout
    finally:  # back to exactly one row
        query(f"DELETE FROM {FACT} WHERE order_id = ?", (oid,))
        query(f"INSERT INTO {FACT} SELECT * FROM {keep}")
        query(f"DROP TABLE {keep}")


def test_source_freshness_is_checked():
    """AC12: freshness on silver warns after 10 min and fails after 30 min."""
    out = subprocess.run(
        [*COMPOSE, "run", "--rm", "--no-deps", "dbt", "--freshness"],
        cwd=ROOT, capture_output=True, text=True,
    )  # fmt: skip
    for source in ("silver.orders", "silver.order_status_history", "silver.gps_pings"):
        assert f"freshness of {source}" in out.stdout

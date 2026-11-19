"""Table maintenance: which statement runs, on which table, and when (spec 005, fake Trino)."""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

_PATH = Path(__file__).resolve().parents[2] / "infra/local/maintenance/maintain.py"
_spec = importlib.util.spec_from_file_location("maintain", _PATH)
m = importlib.util.module_from_spec(_spec)
sys.modules["maintain"] = m  # dataclasses look the module up by name
_spec.loader.exec_module(m)

NOW = datetime(2026, 11, 19, 8, 0, tzinfo=UTC)


class FakeTrino:
    def __init__(self, fail_on: str | None = None, rows=None):
        self.calls: list[tuple[str, dict]] = []
        self.fail_on, self.rows = fail_on, rows or []

    def __call__(self, sql: str, session: dict | None = None) -> list[list]:
        self.calls.append((sql, session or {}))
        if self.fail_on and self.fail_on in sql:
            raise m.TrinoError("Failed to commit: conflict")
        if sql.startswith("SELECT task"):
            return self.rows
        return [["rewritten_data_files_count", 3]]

    def statements(self) -> list[str]:
        return [s for s, _ in self.calls if not s.startswith("INSERT INTO lakehouse.ops")]


def test_every_flink_written_table_is_compacted_silver_more_often():
    assert set(m.COMPACT["silver"].tables) == {
        "silver.gps_pings",
        "silver.orders",
        "silver.order_status_history",
    }
    assert set(m.COMPACT["bronze"].tables) == {
        "bronze.gps_pings",
        "bronze.orders_cdc",
        "bronze.depots_cdc",
        "bronze.vans_cdc",
        "bronze.dlq_events",
    }
    assert m.COMPACT["silver"].every < m.COMPACT["bronze"].every


def test_retention_follows_nfr5_and_cuts_on_partition_boundaries():
    assert m.RETENTION["bronze.gps_pings"] == m.Retention("event_ts", 90, "day")
    assert m.RETENTION["bronze.orders_cdc"] == m.Retention("commit_ts", 365, "day")
    assert m.RETENTION["bronze.dlq_events"] == m.Retention("ingest_ts", 365, "day")
    assert m.RETENTION["silver.gps_pings"] == m.Retention("event_ts", 365, "day")
    assert m.RETENTION["silver.orders"] == m.Retention("created_at", 365, "month")
    assert m.RETENTION["silver.order_status_history"] == m.Retention("status_ts", 365, "day")
    sql = m.retention_sql("silver.orders", m.RETENTION["silver.orders"])
    assert sql == (
        "DELETE FROM lakehouse.silver.orders WHERE created_at < "
        "date_trunc('month', current_timestamp - INTERVAL '365' DAY)"
    )


def test_statements():
    assert m.statement("compact", "silver.gps_pings") == (
        "ALTER TABLE lakehouse.silver.gps_pings EXECUTE optimize(file_size_threshold => '128MB')"
    )
    assert m.statement("manifests", "bronze.gps_pings") == (
        "ALTER TABLE lakehouse.bronze.gps_pings EXECUTE optimize_manifests"
    )
    assert m.statement("expire", "bronze.gps_pings", retention="7d") == (
        "ALTER TABLE lakehouse.bronze.gps_pings "
        "EXECUTE expire_snapshots(retention_threshold => '7d')"
    )
    assert m.statement("orphans", "bronze.gps_pings", retention="7d") == (
        "ALTER TABLE lakehouse.bronze.gps_pings "
        "EXECUTE remove_orphan_files(retention_threshold => '7d')"
    )


def test_expiry_session_allows_exactly_the_configured_retention():
    trino = FakeTrino()
    m.run_task(trino, "expire", ["e2e.t"], NOW, retention="0s")
    (sql, session), _log = trino.calls
    assert "expire_snapshots(retention_threshold => '0s')" in sql
    assert session == {"lakehouse.expire_snapshots_min_retention": "0s"}


def test_each_run_is_logged_and_a_failure_does_not_stop_the_others():
    trino = FakeTrino(fail_on="silver.orders ")
    results = m.run_task(trino, "compact", ["silver.gps_pings", "silver.orders"], NOW)
    assert [r.status for r in results] == ["ok", "failed"]
    logs = [s for s, _ in trino.calls if s.startswith("INSERT INTO lakehouse.ops")]
    assert len(logs) == 2
    assert "'failed'" in logs[1] and "conflict" in logs[1]


def test_log_values_are_escaped():
    sql = m.log_sql(m.Result("compact", "x.y", NOW, NOW, "failed", "it's broken"))
    assert "'it''s broken'" in sql


def test_due_tasks_use_the_last_run_from_the_log():
    last = {
        "compact:silver": NOW - timedelta(minutes=10),
        "compact:bronze": NOW - timedelta(hours=2),
    }
    due = m.due_tasks(last, NOW)
    assert "compact:silver" not in due
    assert "compact:bronze" in due
    assert {"manifests", "expire", "orphans", "retention"} <= set(due)  # never ran


def test_last_runs_are_read_from_the_log():
    rows = [["compact:silver", "2026-11-19 07:50:00.000000 UTC"]]
    last = m.last_runs(FakeTrino(rows=rows))
    assert last == {"compact:silver": datetime(2026, 11, 19, 7, 50, tzinfo=UTC)}


def test_once_runs_only_the_requested_task_and_table():
    trino = FakeTrino()
    m.run_once(trino, NOW, task="compact", table="silver.gps_pings")
    assert trino.statements() == [m.statement("compact", "silver.gps_pings")]


@pytest.mark.parametrize("task", ["retention"])
def test_retention_on_a_table_without_a_rule_is_refused(task):
    with pytest.raises(SystemExit):
        m.run_once(FakeTrino(), NOW, task=task, table="bronze.vans_cdc")

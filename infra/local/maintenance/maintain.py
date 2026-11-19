"""Iceberg table maintenance through Trino (spec 005, ADR 0012). Stdlib only.

Streaming writers commit every 30 s, so tables collect small files, upsert delete files, manifests
and snapshots. This loop keeps them healthy:

  task        tables                         every    what
  compact     silver (3 tables)              30 min   rewrite small files, apply deletes, sort
  compact     bronze (5 tables)              60 min   same
  manifests   all                            daily    merge manifests
  expire      all                            daily    drop snapshots older than 7 days
  orphans     all                            daily    delete unreferenced files older than 7 days
  retention   tables with a rule (NFR-5)     daily    drop whole partitions past their retention

Each run is logged to lakehouse.ops.maintenance_runs. A failed run (for example a commit conflict
with a streaming writer) is logged and retried at the next interval; it never stops the loop.

  python maintain.py                       # loop forever (the `maintenance` service)
  python maintain.py --once [--task T] [--table ns.t]   # `make maintain`
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

TRINO_URL = os.environ.get("TRINO_URL", "http://trino:8080")
SNAPSHOT_RETENTION = os.environ.get("SNAPSHOT_RETENTION", "7d")
ORPHAN_RETENTION = os.environ.get("ORPHAN_RETENTION", "7d")
TARGET_FILE = "128MB"  # files below this are rewritten; Iceberg's own target is 512 MB


def _minutes(name: str, default: int) -> timedelta:
    return timedelta(minutes=int(os.environ.get(name, default)))


@dataclass(frozen=True)
class Schedule:
    tables: tuple[str, ...]
    every: timedelta


@dataclass(frozen=True)
class Retention:
    column: str
    days: int
    grain: str  # the partition transform, so the cut falls on a partition boundary


SILVER = ("silver.gps_pings", "silver.orders", "silver.order_status_history")
BRONZE = (
    "bronze.gps_pings",
    "bronze.orders_cdc",
    "bronze.depots_cdc",
    "bronze.vans_cdc",
    "bronze.dlq_events",
)
ALL_TABLES = (*SILVER, *BRONZE, "ops.maintenance_runs")
DAILY = timedelta(days=1)
COMPACT = {
    "silver": Schedule(SILVER, _minutes("COMPACT_SILVER_MINUTES", 30)),
    "bronze": Schedule(BRONZE, _minutes("COMPACT_BRONZE_MINUTES", 60)),
}
# NFR-5. depots_cdc and vans_cdc are unpartitioned and tiny: their whole history is kept.
RETENTION = {
    "bronze.gps_pings": Retention("event_ts", 90, "day"),
    "bronze.orders_cdc": Retention("commit_ts", 365, "day"),
    "bronze.dlq_events": Retention("ingest_ts", 365, "day"),
    "silver.gps_pings": Retention("event_ts", 365, "day"),
    "silver.orders": Retention("created_at", 365, "month"),
    "silver.order_status_history": Retention("status_ts", 365, "day"),
}
SCHEDULES = {
    "compact:silver": COMPACT["silver"],
    "compact:bronze": COMPACT["bronze"],
    "manifests": Schedule(ALL_TABLES, DAILY),
    "expire": Schedule(ALL_TABLES, DAILY),
    "orphans": Schedule(ALL_TABLES, DAILY),
    "retention": Schedule(tuple(RETENTION), DAILY),
}

Trino = Callable[..., list[list]]


class TrinoError(RuntimeError):
    pass


def trino_call(sql: str, session: dict | None = None) -> list[list]:
    """Run one statement through the Trino HTTP protocol and return all rows."""
    headers = {"X-Trino-User": "maintenance", "X-Trino-Catalog": "lakehouse"}
    if session:
        headers["X-Trino-Session"] = ",".join(f"{k}={v}" for k, v in session.items())
    req = urllib.request.Request(
        f"{TRINO_URL}/v1/statement", data=sql.encode(), headers=headers, method="POST"
    )
    rows: list[list] = []
    while True:
        with urllib.request.urlopen(req, timeout=60) as r:
            reply = json.load(r)
        if "error" in reply:
            raise TrinoError(reply["error"].get("message", "query failed"))
        rows.extend(reply.get("data", []))
        if "nextUri" not in reply:
            return rows
        req = urllib.request.Request(reply["nextUri"], headers=headers)
        time.sleep(0.05)


def _alter(table: str, procedure: str) -> str:
    return f"ALTER TABLE lakehouse.{table} EXECUTE {procedure}"


def retention_sql(table: str, rule: Retention) -> str:
    return (
        f"DELETE FROM lakehouse.{table} WHERE {rule.column} < "
        f"date_trunc('{rule.grain}', current_timestamp - INTERVAL '{rule.days}' DAY)"
    )


def statement(task: str, table: str, retention: str = "7d") -> str:
    if task == "compact":
        return _alter(table, f"optimize(file_size_threshold => '{TARGET_FILE}')")
    if task == "manifests":
        return _alter(table, "optimize_manifests")
    if task == "expire":
        return _alter(table, f"expire_snapshots(retention_threshold => '{retention}')")
    if task == "orphans":
        return _alter(table, f"remove_orphan_files(retention_threshold => '{retention}')")
    if task == "retention":
        return retention_sql(table, RETENTION[table])
    raise ValueError(f"unknown task {task!r}")


def session_for(task: str, retention: str) -> dict:
    # Trino refuses a threshold below its minimum (7 d by default); allow the configured one.
    if task == "expire":
        return {"lakehouse.expire_snapshots_min_retention": retention}
    if task == "orphans":
        return {"lakehouse.remove_orphan_files_min_retention": retention}
    return {}


@dataclass(frozen=True)
class Result:
    task: str
    table: str
    started: datetime
    finished: datetime
    status: str
    detail: str


def _lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _ts(value: datetime) -> str:
    return f"TIMESTAMP '{value.astimezone(UTC):%Y-%m-%d %H:%M:%S.%f} UTC'"


def log_sql(r: Result) -> str:
    return (
        "INSERT INTO lakehouse.ops.maintenance_runs VALUES ("
        f"{_lit(r.task)}, {_lit(r.table)}, {_ts(r.started)}, {_ts(r.finished)}, "
        f"{_lit(r.status)}, {_lit(r.detail[:1000])})"
    )


def run_task(
    trino: Trino,
    task: str,
    tables: list[str] | tuple[str, ...],
    now: datetime,
    retention: str | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> list[Result]:
    if retention is None:
        retention = ORPHAN_RETENTION if task == "orphans" else SNAPSHOT_RETENTION
    results = []
    for table in tables:
        started = now if not results else clock()
        try:
            rows = trino(statement(task, table, retention), session_for(task, retention))
            status, detail = "ok", json.dumps(rows, default=str)
        except Exception as exc:  # noqa: BLE001 - logged and retried at the next interval
            status, detail = "failed", f"{type(exc).__name__}: {exc}"
        result = Result(task, table, started, clock(), status, detail)
        try:
            trino(log_sql(result), None)
        except Exception as exc:  # noqa: BLE001 - the log is best effort
            print(f"could not log {task} {table}: {exc}", file=sys.stderr)
        print(f"{task:<9} {table:<30} {status} {detail[:120]}", flush=True)
        results.append(result)
    return results


def _parse_ts(text: str) -> datetime:
    stamp = text.removesuffix(" UTC")
    return datetime.strptime(stamp[:26], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=UTC)


def last_runs(trino: Trino) -> dict[str, datetime]:
    """When each schedule last ran, from the log (compact is tracked per layer)."""
    rows = trino(
        "SELECT task, CAST(max(started_at) AS varchar) FROM ("
        "SELECT CASE WHEN task = 'compact' THEN 'compact:' || split_part(table_name, '.', 1) "
        "ELSE task END AS task, started_at FROM lakehouse.ops.maintenance_runs) GROUP BY 1",
        None,
    )
    return {key: _parse_ts(ts) for key, ts in rows}


def due_tasks(last: dict[str, datetime], now: datetime) -> list[str]:
    return [k for k, s in SCHEDULES.items() if k not in last or now - last[k] >= s.every]


def run_once(trino: Trino, now: datetime, task: str | None = None, table: str | None = None):
    """Run every task (or one), on its own tables (or one table), now."""
    tasks = list(dict.fromkeys(k.split(":")[0] for k in SCHEDULES))
    if task is not None and task not in tasks:
        raise SystemExit(f"unknown task {task!r}; one of {tasks}")
    if table and task == "retention" and table not in RETENTION:
        raise SystemExit(f"{table} has no retention rule")
    for name in [task] if task else tasks:
        if table:
            tables = [table]
        else:
            tables = [t for k, s in SCHEDULES.items() if k.split(":")[0] == name for t in s.tables]
        run_task(trino, name, tables, now)


def loop(trino: Trino) -> None:
    while True:
        now = datetime.now(UTC)
        try:
            last = last_runs(trino)
        except Exception as exc:  # noqa: BLE001 - Trino still starting
            print(f"waiting for Trino: {exc}", file=sys.stderr)
            time.sleep(30)
            continue
        for key in due_tasks(last, now):
            run_task(trino, key.split(":")[0], SCHEDULES[key].tables, now)
        time.sleep(60)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true", help="run now and exit")
    parser.add_argument("--task", help="compact, manifests, expire, orphans or retention")
    parser.add_argument("--table", help="only this table, e.g. silver.gps_pings")
    args = parser.parse_args(argv)
    if args.once:
        run_once(trino_call, datetime.now(UTC), args.task or None, args.table or None)
    else:
        loop(trino_call)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

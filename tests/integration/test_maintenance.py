"""Spec 005 acceptance tests for table maintenance (AC5-AC8, AC10, AC12), against `make up`."""

from __future__ import annotations

import json
import os
import subprocess
import threading
import urllib.request
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from conftest import (
    COMPOSE,
    ROOT,
    SILVER_GPS,
    TABLE,
    count_table,
    pg_query,
    query,
    run_gps_sim,
    run_orders_sim,
    send_ping,
    wait_for,
)

from simulator.fleet import Ping

pytestmark = pytest.mark.e2e

SILVER_TIMEOUT_S = 240
JOBS = {"bronze_gps", "bronze_cdc", "silver_gps", "silver_orders"}


def running_jobs() -> set[str]:
    # job_states() keeps one state per name, and an earlier run stopped with a savepoint is
    # FINISHED under the same name; ask which names have a RUNNING run.
    with urllib.request.urlopen("http://localhost:8081/jobs/overview", timeout=5) as r:
        return {j["name"] for j in json.load(r)["jobs"] if j["state"] == "RUNNING"}


def maintain(task: str, table: str | None = None, **env: str) -> str:
    cmd = ["make", "-s", "maintain", f"TASK={task}"] + ([f"TABLE={table}"] if table else [])
    out = subprocess.run(
        cmd, cwd=ROOT, check=True, capture_output=True, text=True, env={**os.environ, **env}
    )
    return out.stdout


def _files_covering(van: str, day: str) -> int:
    """Data files of one silver GPS day whose vehicle_id min/max include `van`: what a query for
    that van and day must open (Trino skips the others by these bounds)."""
    return query(
        'SELECT count(*) FROM lakehouse.silver."gps_pings$files" WHERE content = 0 '
        "AND json_extract_scalar(readable_metrics, '$.vehicle_id.lower_bound') <= ? "
        "AND json_extract_scalar(readable_metrics, '$.vehicle_id.upper_bound') >= ? "
        "AND CAST(json_extract_scalar(readable_metrics, '$.event_ts.lower_bound') AS varchar) "
        "LIKE ?",
        (van, van, f"{day}%"),
    )[0][0]


def _checksum(table: str) -> tuple:
    return query(
        f"SELECT count(*), checksum(ROW(vehicle_id, event_ts, lat, lon, ingest_ts, lateness_s)) "
        f"FROM lakehouse.{table}"
    )[0]


def _van_rows(van: str) -> list[tuple]:
    return query(
        f"SELECT event_ts, lat, lon, ingest_ts FROM {SILVER_GPS} WHERE vehicle_id = ? ORDER BY 1",
        (van,),
    )


def test_compaction_reads_fewer_files_for_one_van_and_keeps_every_row(prefix):
    """AC5 + AC6: after compaction a one-van query opens fewer files; rows are identical."""
    sent, _ = run_gps_sim(prefix, vans=2, hours=0.25)
    wait_for(lambda: count_table(SILVER_GPS, prefix), lambda n: n == sent, SILVER_TIMEOUT_S)
    van = f"{prefix}-0001"
    last = _van_rows(van)[-1][0]
    # a second commit that also holds this van, so its rows are spread over two files at least
    extra = Ping(van, last + timedelta(seconds=5), 10.78, 106.69, 20.0, 90.0)
    send_ping(extra, extra.event_ts)
    wait_for(lambda: count_table(SILVER_GPS, prefix), lambda n: n == sent + 1, SILVER_TIMEOUT_S)
    day = f"{extra.event_ts.astimezone(UTC):%Y-%m-%d}"

    before_files = _files_covering(van, day)
    before_rows, (before_count, _) = _van_rows(van), _checksum("silver.gps_pings")
    before_deletes = query(
        'SELECT count(*) FROM lakehouse.silver."gps_pings$files" WHERE content <> 0'
    )[0][0]
    maintain("compact", "silver.gps_pings")
    after_files = _files_covering(van, day)

    assert before_files >= 2
    assert after_files < before_files
    assert _van_rows(van) == before_rows
    count, _ = _checksum("silver.gps_pings")
    assert count >= before_count  # rows arriving meanwhile can only add; compaction removes none
    after_deletes = query(
        'SELECT count(*) FROM lakehouse.silver."gps_pings$files" WHERE content <> 0'
    )[0][0]
    assert after_deletes <= before_deletes


def test_compaction_during_live_writes_keeps_silver_exact(prefix):
    """AC7: compaction runs three times while GPS and order changes stream in."""
    errors: list[BaseException] = []

    def compact_three_times() -> None:
        try:
            for _ in range(3):
                maintain("compact")
        except BaseException as exc:  # noqa: BLE001 - re-raised in the test thread
            errors.append(exc)

    worker = threading.Thread(target=compact_three_times)
    worker.start()
    sent, chaos = run_gps_sim(prefix, "--chaos", vans=3, hours=1.0)
    orders = f"ORD{uuid.uuid4().hex[:6].upper()}"
    run_orders_sim(orders, vans=4, orders_per_day=40, hours=12)
    worker.join()
    assert not errors, errors

    expected = sent - chaos.counts["copies"]
    wait_for(lambda: count_table(SILVER_GPS, prefix), lambda n: n == expected, SILVER_TIMEOUT_S)
    rows, keys = query(
        f"SELECT count(*), count(DISTINCT (vehicle_id, event_ts)) FROM {SILVER_GPS} "
        "WHERE vehicle_id LIKE ?",
        (f"{prefix}-%",),
    )[0]
    assert rows == keys == expected

    latest = (
        'SELECT "after".order_id, max_by("after".status, source_lsn), max(source_lsn) '
        'FROM lakehouse.bronze.orders_cdc WHERE "after".order_id LIKE ? GROUP BY 1'
    )
    # the order changes reach bronze through Debezium and bronze_cdc (one 30 s commit or more)
    in_db = pg_query("SELECT order_id, status FROM orders WHERE order_id LIKE %s", (f"{orders}%",))
    want, _ = wait_for(
        lambda: sorted(query(latest, (f"{orders}%",))),
        lambda rows: sorted((oid, st) for oid, st, _ in rows) == sorted(in_db),
        SILVER_TIMEOUT_S,
    )
    wait_for(
        lambda: sorted(
            query(
                "SELECT order_id, status, last_lsn FROM lakehouse.silver.orders "
                "WHERE order_id LIKE ?",
                (f"{orders}%",),
            )
        ),
        lambda got: got == want,
        SILVER_TIMEOUT_S,
    )
    ok_runs = query(
        "SELECT count(*) FROM lakehouse.ops.maintenance_runs WHERE task = 'compact' "
        "AND table_name = 'silver.gps_pings' AND status = 'ok' "
        "AND started_at > current_timestamp - INTERVAL '30' MINUTE"
    )[0][0]
    assert ok_runs >= 3
    assert running_jobs() >= JOBS


def _aws(*args: str) -> str:
    out = subprocess.run(
        [*COMPOSE, "run", "--rm", "--no-deps", "--entrypoint", "aws", "minio-init",
         "--endpoint-url", "http://minio:9000", *args],
        cwd=ROOT, check=True, capture_output=True, text=True,
    )  # fmt: skip
    return out.stdout


@pytest.fixture
def scratch_table():
    """A table only this test writes to, so expiring with 0 s cannot touch a streaming job."""
    name = f"t_{uuid.uuid4().hex[:8]}"
    query("CREATE SCHEMA IF NOT EXISTS lakehouse.e2e")
    query(f"CREATE TABLE lakehouse.e2e.{name} (id integer, note varchar)")
    for i in range(3):
        query(f"INSERT INTO lakehouse.e2e.{name} VALUES ({i}, 'row {i}')")
    yield f"e2e.{name}"
    query(f"DROP TABLE IF EXISTS lakehouse.e2e.{name}")


def test_expiry_and_orphan_removal(scratch_table):
    """AC8: old snapshots and unreferenced files go; current rows stay."""
    ns, name = scratch_table.split(".")
    snaps = f'lakehouse.{ns}."{name}$snapshots"'
    assert query(f"SELECT count(*) FROM {snaps}")[0][0] >= 3
    path = query(f'SELECT "$path" FROM lakehouse.{scratch_table} LIMIT 1')[0][0]
    data_dir = path.rsplit("/", 1)[0]
    orphan = f"{data_dir}/orphan-{uuid.uuid4().hex[:6]}.parquet"
    subprocess.run(
        [*COMPOSE, "run", "--rm", "--no-deps", "-T", "--entrypoint", "sh", "minio-init", "-c",
         f"echo junk | aws --endpoint-url http://minio:9000 s3 cp - {orphan}"],
        cwd=ROOT, check=True, capture_output=True, text=True,
    )  # fmt: skip
    assert orphan.rsplit("/", 1)[1] in _aws("s3", "ls", f"{data_dir}/")

    maintain("expire", scratch_table, SNAPSHOT_RETENTION="0s")
    maintain("orphans", scratch_table, ORPHAN_RETENTION="0s")

    assert query(f"SELECT count(*) FROM {snaps}")[0][0] == 1
    assert orphan.rsplit("/", 1)[1] not in _aws("s3", "ls", f"{data_dir}/")
    assert query(f"SELECT count(*) FROM lakehouse.{scratch_table}")[0][0] == 3


def test_retention_drops_partitions_past_their_age_and_jobs_keep_running(prefix):
    """AC8: a ping 400 days old is dropped from bronze (90 d) and silver (365 d); today's stays.
    The retention delete on bronze must not stop the silver job reading bronze."""
    now = datetime.now(UTC).replace(microsecond=0)
    old = Ping(f"{prefix}-0001", now - timedelta(days=400), 10.78, 106.69, 20.0, 90.0)
    new = Ping(f"{prefix}-0001", now - timedelta(minutes=1), 10.78, 106.69, 20.0, 90.0)
    for ping in (old, new):
        send_ping(ping, now)
    wait_for(lambda: count_table(SILVER_GPS, prefix), lambda n: n == 2, SILVER_TIMEOUT_S)

    maintain("retention")

    for table in (TABLE, SILVER_GPS):
        kept = query(f"SELECT event_ts FROM {table} WHERE vehicle_id LIKE ?", (f"{prefix}-%",))
        assert [r[0] for r in kept] == [new.event_ts], table
    # the silver job polls bronze every 10 s; give it a few polls past the delete snapshot
    later = Ping(f"{prefix}-0002", now, 10.78, 106.69, 20.0, 90.0)
    send_ping(later, now)
    wait_for(lambda: count_table(SILVER_GPS, prefix), lambda n: n == 2, SILVER_TIMEOUT_S)
    assert running_jobs() >= JOBS


def _catalog(name: str) -> dict:
    ns, table = name.split(".")
    url = f"http://localhost:8181/v1/namespaces/{ns}/tables/{table}"
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.load(r)["metadata"]


def _metadata_files(location: str) -> int:
    listing = _aws("s3", "ls", f"{location}/metadata/")
    return sum(1 for line in listing.splitlines() if line.endswith(".metadata.json"))


def test_metadata_files_and_manifests_stay_bounded():
    """AC12: each commit drops the metadata version that falls out of the last 100, so the count
    of metadata files stops growing (files from before the setting go with orphan removal), and
    manifests are merged."""
    meta = _catalog("bronze.gps_pings")
    assert meta["properties"]["write.metadata.delete-after-commit.enabled"] == "true"
    assert meta["properties"]["write.metadata.previous-versions-max"] == "100"
    assert len(meta["metadata-log"]) <= 100
    before = _metadata_files(meta["location"])
    first = meta["metadata-log"][-1]["metadata-file"]

    # Force at least two more commits: an idle Flink sink commits only every 10th checkpoint.
    def log_after_a_ping() -> list[str]:
        now = datetime.now(UTC).replace(microsecond=0)
        send_ping(Ping(f"E2E-META-{uuid.uuid4().hex[:6]}", now, 10.78, 106.69, 10.0, 0.0), now)
        return [e["metadata-file"] for e in _catalog("bronze.gps_pings")["metadata-log"]]

    wait_for(
        log_after_a_ping,
        lambda log: first in log and len(log) - 1 - log.index(first) >= 2,
        240,
        every_s=20,
    )
    assert _metadata_files(meta["location"]) <= before
    maintain("manifests", "bronze.gps_pings")
    assert query('SELECT count(*) FROM lakehouse.bronze."gps_pings$manifests"')[0][0] <= 3


def test_table_health_matches_the_table_metadata():
    """AC10."""
    out = subprocess.run(
        ["make", "-s", "table-health"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout
    rows = {}
    for line in out.splitlines():
        cells = [c.strip() for c in line.split("|")]
        if len(cells) == 10 and "." in cells[0]:
            rows[cells[0]] = cells
    assert len(rows) == 9
    data, deletes = query(
        'SELECT count_if(content = 0), count_if(content <> 0) FROM lakehouse.silver."orders$files"'
    )[0]
    health = rows["silver.orders"]
    # a commit can land between the two reads; allow one commit's worth of difference
    assert abs(int(health[1]) - data) <= 2 and abs(int(health[2]) - deletes) <= 2
    assert health[5] != "NULL", "compaction has run at least once (maintenance service)"

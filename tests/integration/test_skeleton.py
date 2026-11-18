"""Spec 001 acceptance tests against the running local stack (`make up`, then `make e2e`)."""

from __future__ import annotations

import re
import time

import pytest
from conftest import (
    BOOTSTRAP,
    TABLE,
    TOPIC,
    compose,
    compose_services,
    count_prefix,
    produce,
    query,
    wait_for,
)

pytestmark = pytest.mark.e2e

LONG_RUNNING = {"minio", "redpanda", "iceberg-rest", "trino", "jobmanager", "taskmanager"}
ONE_SHOT = {"minio-init", "redpanda-init", "trino-init"}
CHECKPOINT_S = 30


def test_stack_healthy_topic_and_table_exist():
    """AC1 (the < 5 min bound is enforced by CI: `timeout 300 make up`)."""
    services = {s["Service"]: s for s in compose_services()}
    for name in LONG_RUNNING:
        assert services[name]["State"] == "running", name
        assert services[name].get("Health", "") in ("", "healthy"), name
    for name in ONE_SHOT:
        assert services[name]["State"] == "exited" and services[name]["ExitCode"] == 0, name

    from confluent_kafka.admin import AdminClient

    topics = AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=10).topics
    assert TOPIC in topics
    assert len(topics[TOPIC].partitions) == 3
    assert ("gps_pings",) in query("SHOW TABLES FROM lakehouse.bronze")


def test_exactly_n_rows(prefix):
    """AC3: 20 vans x 2 min at 5 s = 480 pings in -> exactly 480 rows, no dups."""
    sent = produce(prefix, vans=20, steps=24)
    assert sent == 480
    wait_for(lambda: count_prefix(prefix), lambda n: n >= sent, timeout_s=120)
    time.sleep(CHECKPOINT_S + 5)  # one more checkpoint: nothing extra may appear
    rows = query(
        f"SELECT count(*), count(DISTINCT (kafka_partition, kafka_offset)) FROM {TABLE} "
        "WHERE vehicle_id LIKE ?",
        (f"{prefix}-%",),
    )
    assert rows[0] == (sent, sent)


def test_freshness_under_60s(prefix):
    """AC4: a produced ping is queryable in bronze within 60 s."""
    produce(prefix, vans=1, steps=1)
    _, waited = wait_for(lambda: count_prefix(prefix), lambda n: n == 1, timeout_s=90, every_s=1)
    assert waited <= 60, f"took {waited:.1f}s"


def test_bronze_schema_and_partitioning(prefix):
    """AC5: payload fields + ingest_ts + Kafka partition/offset; partitioned by event day."""
    columns = [r[0] for r in query(f"DESCRIBE {TABLE}")]
    assert columns == [
        "vehicle_id",
        "event_ts",
        "lat",
        "lon",
        "speed_kmh",
        "heading_deg",
        "ingest_ts",
        "kafka_partition",
        "kafka_offset",
    ]
    ddl = query(f"SHOW CREATE TABLE {TABLE}")[0][0]
    assert "day(event_ts)" in ddl

    produce(prefix, vans=2, steps=3)
    wait_for(lambda: count_prefix(prefix), lambda n: n == 6, timeout_s=90)
    rows = query(
        f"SELECT ingest_ts >= event_ts, kafka_partition, kafka_offset FROM {TABLE} "
        "WHERE vehicle_id LIKE ?",
        (f"{prefix}-%",),
    )
    for ingest_after_event, partition, offset in rows:
        assert ingest_after_event
        assert partition in (0, 1, 2)
        assert offset >= 0


def _mem_bytes(text: str) -> float:
    value, unit = re.fullmatch(r"([\d.]+)\s*([KMG]i?B|B)", text.strip()).groups()
    scale = {"B": 1, "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "KB": 1e3, "MB": 1e6, "GB": 1e9}
    return float(value) * scale[unit]


def test_stack_memory_under_6gb(prefix):
    """AC8: total container memory stays under 6 GB while handling dev-size load."""
    produce(prefix, vans=20, steps=24)
    ids = compose("ps", "-q").stdout.split()
    out = compose("stats", "--no-stream", "--format", "{{.MemUsage}}", *ids, check=False).stdout
    if not out.strip():  # older compose without `stats`: fall back to docker stats
        import subprocess

        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", *ids],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    used = sum(_mem_bytes(line.split("/")[0]) for line in out.splitlines() if line.strip())
    assert used < 6 * 2**30, f"stack uses {used / 2**30:.2f} GiB"


@pytest.mark.restart
def test_data_survives_restart(prefix):
    """AC7: stop the stack (keeping volumes), start it again, earlier rows are still there."""
    sent = produce(prefix, vans=5, steps=4)
    wait_for(lambda: count_prefix(prefix), lambda n: n == sent, timeout_s=120)

    compose("--profile", "jobs", "down")
    compose("up", "-d", "--remove-orphans")
    compose("run", "--rm", "flink-submit")

    rows, _ = wait_for(lambda: _safe_count(prefix), lambda n: n is not None, timeout_s=180)
    assert rows == sent

    # the pipeline keeps working after the restart
    produce(f"{prefix}-after", vans=1, steps=2)
    wait_for(lambda: count_prefix(f"{prefix}-after"), lambda n: n == 2, timeout_s=120)


def _safe_count(prefix: str) -> int | None:
    try:
        return count_prefix(prefix)
    except Exception:  # Trino still starting
        return None

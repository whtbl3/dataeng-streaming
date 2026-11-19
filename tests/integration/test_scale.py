"""Spec 005 tests: Kafka, Flink parallelism and state (AC1, AC4, AC9); needs `make up`."""

from __future__ import annotations

import json
import os
import urllib.request
import uuid

import pytest
from conftest import BOOTSTRAP, TABLE, TOPIC, query, run_gps_sim, wait_for

pytestmark = pytest.mark.e2e

GPS_PARTITIONS = 6
MIB = 1024 * 1024
# Per-partition size bounds set by infra/local/redpanda/init.sh (spec 005 resolved question 5).
SIZE_BOUNDS = {
    TOPIC: 1024 * MIB,
    f"dlq.{TOPIC}": 128 * MIB,
    "cdc.public.orders": 256 * MIB,
    "dlq.cdc.public.orders": 128 * MIB,
}
SEVEN_DAYS_MS = 7 * 24 * 3600 * 1000
GPS_PARALLELISM = int(os.environ.get("GPS_PARALLELISM", "2"))
BRONZE_TIMEOUT_S = 120


def _admin():
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def _topic_config(topic: str) -> dict[str, str]:
    from confluent_kafka.admin import ConfigResource

    res = ConfigResource(ConfigResource.Type.TOPIC, topic)
    admin = _admin()  # keep a reference until the future resolves
    configs = admin.describe_configs([res])[res].result(timeout=10)
    return {k: v.value for k, v in configs.items()}


def _flink(path: str):
    with urllib.request.urlopen(f"http://localhost:8081{path}", timeout=5) as r:
        return json.load(r)


def test_gps_topic_and_its_dlq_have_6_partitions():
    meta = _admin().list_topics(timeout=10).topics
    for topic in (TOPIC, f"dlq.{TOPIC}"):
        assert len(meta[topic].partitions) == GPS_PARTITIONS, topic


def test_topics_are_bounded_by_size_and_time():
    for topic, bound in SIZE_BOUNDS.items():
        cfg = _topic_config(topic)
        assert int(cfg["retention.bytes"]) == bound, topic
        assert int(cfg["retention.ms"]) == SEVEN_DAYS_MS, topic


def test_a_van_stays_on_one_partition(prefix):
    pings, _ = run_gps_sim(prefix, vans=6, hours=0.25)
    rows, _ = wait_for(
        lambda: query(
            f"SELECT vehicle_id, count(*), count(DISTINCT kafka_partition), min(kafka_partition) "
            f"FROM {TABLE} WHERE vehicle_id LIKE ? GROUP BY 1",
            (f"{prefix}-%",),
        ),
        lambda r: sum(n for _, n, _, _ in r) == pings,
        BRONZE_TIMEOUT_S,
    )
    assert all(parts == 1 for _, _, parts, _ in rows), rows
    assert len({p for *_, p in rows}) >= 2, "six vans should spread over more than one partition"


@pytest.mark.parametrize("job", ["bronze_gps", "silver_gps"])
def test_gps_jobs_run_with_configured_parallelism(job):
    jobs = [j for j in _flink("/jobs/overview")["jobs"] if j["name"] == job]
    running = [j for j in jobs if j["state"] == "RUNNING"]
    assert running, f"{job} is not running"
    vertices = _flink(f"/jobs/{running[0]['jid']}")["vertices"]
    assert max(v["parallelism"] for v in vertices) == GPS_PARALLELISM


def test_size_bound_drops_the_oldest_segments():
    """AC9 on a small scratch topic: the same mechanism as the real topics, at 2 MiB."""
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import NewTopic

    topic = f"e2e-bound-{uuid.uuid4().hex[:8]}"
    bound, segment = 2 * MIB, 1 * MIB
    admin = _admin()
    admin.create_topics(
        [
            NewTopic(
                topic,
                1,
                1,
                config={"retention.bytes": str(bound), "segment.bytes": str(segment)},
            )
        ]
    )[topic].result(timeout=10)
    try:
        producer = Producer({"bootstrap.servers": BOOTSTRAP, "compression.type": "none"})
        value = os.urandom(10 * 1024)  # incompressible, so bytes on disk ≈ bytes sent
        sent = 1000  # ≈ 10 MiB, five times the bound
        for _ in range(sent):
            producer.produce(topic, value=value)
            producer.poll(0)
        assert producer.flush(30) == 0

        consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": topic})

        def kept() -> int:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=5)
            return high - low

        left, _ = wait_for(kept, lambda n: n * len(value) <= bound + segment, 120, every_s=5)
        consumer.close()
        assert 0 < left < sent
    finally:
        admin.delete_topics([topic])[topic].result(timeout=10)


def test_state_lives_in_rocksdb_with_incremental_checkpoints():
    """AC4: keyed state is on disk, and a checkpoint uploads only the files that changed."""
    cfg = {c["key"]: c["value"] for c in _flink("/jobmanager/config")}
    assert cfg["state.backend.type"] == "rocksdb"
    assert cfg["execution.checkpointing.incremental"] == "true"
    assert cfg["execution.checkpointing.externalized-checkpoint-retention"] == (
        "RETAIN_ON_CANCELLATION"
    )


def _catalog_table(name: str) -> dict:
    namespace, table = name.split(".")
    url = f"http://localhost:8181/v1/namespaces/{namespace}/tables/{table}"
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.load(r)["metadata"]


@pytest.mark.parametrize("table", ["bronze.gps_pings", "silver.gps_pings"])
def test_each_commit_writes_one_file_per_partition(prefix, table):
    """AC11: with hash write distribution, parallel writers share no partition in a commit."""
    sent, _ = run_gps_sim(prefix, vans=6, hours=0.5)
    wait_for(
        lambda: query(
            f"SELECT count(*) FROM lakehouse.{table} WHERE vehicle_id LIKE ?", (f"{prefix}-%",)
        )[0][0],
        lambda n: n == sent,
        240,
    )
    namespace, name = table.split(".")
    rows = query(
        f"SELECT CAST(element_at(summary, 'added-data-files') AS integer), "
        f"CAST(element_at(summary, 'changed-partition-count') AS integer) "
        f'FROM lakehouse.{namespace}."{name}$snapshots" '
        "WHERE element_at(summary, 'added-data-files') IS NOT NULL "
        "AND element_at(summary, 'flink.job-id') IS NOT NULL "
        "ORDER BY committed_at DESC LIMIT 5"
    )
    assert rows, "no Flink commit found"
    assert all(files == parts for files, parts in rows), rows


def test_every_silver_gps_file_uses_the_current_partition_spec():
    """After the layout change an equality delete only reaches rows in current-spec files."""
    current = _catalog_table("silver.gps_pings")["default-spec-id"]
    specs = query(
        'SELECT DISTINCT spec_id FROM lakehouse.silver."gps_pings$files" WHERE content = 0'
    )  # data files; an old-spec delete file left behind applies to nothing and goes next compaction
    assert specs == [(current,)]


def test_silver_gps_is_one_partition_a_day_sorted_by_van():
    meta = _catalog_table("silver.gps_pings")
    spec = next(s for s in meta["partition-specs"] if s["spec-id"] == meta["default-spec-id"])
    assert [f["transform"] for f in spec["fields"]] == ["day"]
    order = next(o for o in meta["sort-orders"] if o["order-id"] == meta["default-sort-order-id"])
    assert len(order["fields"]) == 2

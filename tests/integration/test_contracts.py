"""Spec 003 acceptance tests: contracts, dead-letter routing and evolution (`make up`, `make e2e`).

AC2 (bronze unchanged) and AC9 (memory) are covered by the spec 001/002 tests, which now run
on Avro. AC5 and AC7 are unit tests (`tests/unit/test_compat.py`, `test_invalid_events.py`).
"""

from __future__ import annotations

import subprocess
import uuid
from datetime import UTC, datetime

import pytest
from conftest import (
    BOOTSTRAP,
    REGISTRY,
    ROOT,
    TOPIC,
    count_prefix,
    dlq_rows,
    job_states,
    produce,
    produce_raw,
    query,
    registry_get,
    run_orders_sim,
    wait_for,
)

from contracts.wire import unframe
from simulator.fleet import Ping
from simulator.producer import GpsEncoder

pytestmark = pytest.mark.e2e

CDC_TOPICS = ["cdc.public.depots", "cdc.public.vans", "cdc.public.orders"]
COMMIT_S = 90  # two 30 s checkpoints plus slack


def _jobs_running() -> None:
    states = job_states()
    assert states.get("bronze_gps") == "RUNNING" and states.get("bronze_cdc") == "RUNNING", states


def _latest_values(topic: str, n: int = 5) -> list[bytes]:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"e2e-{uuid.uuid4().hex[:6]}",
            "enable.auto.commit": False,
        }
    )
    try:
        meta = c.list_topics(topic, timeout=10).topics[topic].partitions
        tps = []
        for p in meta:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            if hi > lo:
                tps.append(TopicPartition(topic, p, max(lo, hi - n)))
        c.assign(tps)
        values: list[bytes] = []
        while len(values) < n:
            msg = c.poll(5)
            if msg is None:
                break
            if not msg.error():
                values.append(msg.value())
        return values
    finally:
        c.close()


def test_subjects_registered_full_transitive():
    """AC1: GPS and CDC value subjects exist and enforce FULL_TRANSITIVE."""
    run_orders_sim(f"C{uuid.uuid4().hex[:5]}", vans=1, orders_per_day=2, hours=1)
    subjects, _ = wait_for(
        lambda: set(registry_get("/subjects")),
        lambda s: {f"{t}-value" for t in [TOPIC, *CDC_TOPICS]} <= s,
        timeout_s=60,
    )
    for subject in [f"{t}-value" for t in [TOPIC, *CDC_TOPICS]]:
        assert registry_get(f"/config/{subject}")["compatibilityLevel"] == "FULL_TRANSITIVE"
    assert registry_get(f"/subjects/{TOPIC}-value/versions") == [1, 2]


def test_topics_carry_registry_framed_avro(prefix):
    """AC1: the producer and the CDC connector write Confluent-framed Avro with known ids."""
    produce(prefix, vans=1, steps=2)
    run_orders_sim(f"F{uuid.uuid4().hex[:5]}", vans=1, orders_per_day=2, hours=1)
    for topic in [TOPIC, "cdc.public.orders"]:
        values = _latest_values(topic)
        assert values, f"no messages in {topic}"
        for v in values:
            schema_id, _ = unframe(v)
            assert "schema" in registry_get(f"/schemas/ids/{schema_id}")


@pytest.mark.parametrize("topic", [TOPIC, "cdc.public.orders"])
def test_garbage_goes_to_dlq_once_and_job_keeps_running(topic, prefix):
    """AC3: an undecodable message lands once in the DLQ (table and topic); the next valid
    message still reaches bronze."""
    part, off = produce_raw(topic, prefix.encode(), b'{"looks":"like the old JSON"}')
    produce(prefix, vans=1, steps=1)  # a valid ping right after
    rows, _ = wait_for(lambda: dlq_rows(topic, part, off), lambda r: len(r) >= 1, COMMIT_S)
    assert len(rows) == 1
    reason, detail, schema_id, raw = rows[0]
    assert reason == "not_decodable" and "magic byte" in detail and schema_id is None
    assert bytes(raw) == b'{"looks":"like the old JSON"}'
    wait_for(lambda: count_prefix(prefix), lambda n: n == 1, COMMIT_S)
    _jobs_running()
    dlq = _latest_values(f"dlq.{topic}", n=20)
    assert b'{"looks":"like the old JSON"}' in dlq


def test_rule_breaks_routed_with_reason(prefix):
    """AC4: each broken rule gets its own reason; none of them reach bronze."""
    enc = GpsEncoder.from_registry(REGISTRY)
    now = datetime.now(UTC)
    cases = {
        "missing_vehicle_id": {"vehicle_id": ""},
        "out_of_bbox": {"lat": 21.0285},
        "speed_out_of_range": {"speed_kmh": 150.0},
        "event_ts_in_future": {"event_ts": now.replace(year=now.year + 1)},
    }
    sent = {}
    for reason, change in cases.items():
        rec = enc.record(Ping(f"{prefix}-0001", now, 10.78, 106.69, 30.0, 90.0))
        rec.update(change)
        sent[reason] = produce_raw(TOPIC, prefix.encode(), enc.encode_record(rec))
    for reason, (part, off) in sent.items():
        rows, _ = wait_for(
            lambda p=part, o=off: dlq_rows(TOPIC, p, o), lambda r: len(r) == 1, COMMIT_S
        )
        assert rows[0][0] == reason and rows[0][2] == enc.schema_id
    assert count_prefix(prefix) == 0
    _jobs_running()


def test_v1_and_v2_pings_reach_bronze(prefix):
    """AC6: a producer on the newer schema (v2, extra optional field) and one on v1 both land
    in bronze with the same columns; the job reads both with its v1 reader schema."""
    v1, v2 = GpsEncoder.from_registry(REGISTRY, "v1"), GpsEncoder.from_registry(REGISTRY, "v2")
    assert v1.schema_id != v2.schema_id
    now = datetime.now(UTC).replace(microsecond=0)
    for i, enc in enumerate([v1, v2], start=1):
        key, value = enc.encode(Ping(f"{prefix}-000{i}", now, 10.78, 106.69, 30.0, 90.0))
        produce_raw(TOPIC, key, value)
    wait_for(lambda: count_prefix(prefix), lambda n: n == 2, COMMIT_S)
    rows = query(
        "SELECT vehicle_id, event_ts, lat, speed_kmh FROM lakehouse.bronze.gps_pings "
        "WHERE vehicle_id LIKE ? ORDER BY vehicle_id",
        (f"{prefix}-%",),
    )
    assert [r[0] for r in rows] == [f"{prefix}-0001", f"{prefix}-0002"]
    assert all(r[1] == now and r[2] == 10.78 and r[3] == 30.0 for r in rows)
    _jobs_running()


def test_dlq_status_counts_by_source_and_reason(prefix):
    """AC8: `make dlq-status` shows rejects per source topic and reason."""
    part, off = produce_raw(TOPIC, prefix.encode(), b"not avro")
    wait_for(lambda: dlq_rows(TOPIC, part, off), lambda r: len(r) == 1, COMMIT_S)
    out = subprocess.run(
        ["make", "-s", "dlq-status"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout
    line = next(ln for ln in out.splitlines() if TOPIC in ln and "not_decodable" in ln)
    assert int(line.split("|")[2]) >= 1

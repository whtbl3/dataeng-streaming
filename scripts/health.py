"""`make health` (spec 008): the four NFR-10 conditions, checked now, OK or ALERT each.

  consumer lag grows      per Kafka consumer group: two samples 30 s apart (--quick: one)
  checkpoint failed       per running Flink job: the latest attempt failed after the latest success
  replication slot lag    the CDC slot holds more than 500 MB of WAL
  bronze stuck            GPS messages arrived in the last 2 min, but bronze got no row in 2 min

Exits 1 if any check alerts. The dashboard design (docs/design/dashboards.md) defines the same
rules with the 5-minute window a dashboard tool can keep.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

LAG_MIN = int(os.environ.get("HEALTH_LAG_MIN", "2000"))  # ≈ 50 s of live GPS at dev size
SLOT_MAX_BYTES = 500 * 1024 * 1024
SILENCE = timedelta(minutes=2)
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
FLINK = os.environ.get("FLINK_REST", "http://localhost:8081")
ORDERS_DSN = os.environ.get("ORDERS_DSN", "postgresql://ct:ct-local@localhost:15432/control_tower")
GPS_TOPIC = os.environ.get("GPS_TOPIC", "gps.pings.v1")


# ---- rules (pure) ---------------------------------------------------------------------------
def lag_alert(first: int | None, second: int, minimum: int = LAG_MIN) -> bool:
    return first is not None and second > first and second > minimum


def checkpoint_alert(latest_failed: datetime | None, latest_completed: datetime | None) -> bool:
    return latest_failed is not None and (
        latest_completed is None or latest_failed > latest_completed
    )


def slot_alert(lag_bytes: int | None) -> bool:
    return lag_bytes is not None and lag_bytes > SLOT_MAX_BYTES


def bronze_stuck(
    newest_kafka: datetime | None, newest_bronze: datetime | None, now: datetime
) -> bool:
    if newest_kafka is None or now - newest_kafka > SILENCE:
        return False  # nothing sent lately: idle, not stuck
    return newest_bronze is None or now - newest_bronze > SILENCE


@dataclass(frozen=True)
class Check:
    name: str
    alert: bool
    value: str


def report(checks: list[Check]) -> list[str]:
    return [f"{'ALERT' if c.alert else 'OK':<6} {c.name}  {c.value}" for c in checks]


def exit_code(checks: list[Check]) -> int:
    return 1 if any(c.alert for c in checks) else 0


# ---- collectors -----------------------------------------------------------------------------
def _lags() -> dict[str, int]:
    """Messages not yet committed, per consumer group (high watermark - committed offset)."""
    from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    groups = [g.group_id for g in admin.list_consumer_groups().result(timeout=10).valid]
    probe = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "ct-health-probe"})
    out: dict[str, int] = {}
    for group in sorted(groups):
        if group.startswith(("ct-health", "e2e-")):
            continue
        future = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])[group]
        lag = 0
        for tp in future.result(timeout=10).topic_partitions:
            if tp.offset < 0:
                continue
            _, high = probe.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=5)
            lag += max(high - tp.offset, 0)
        out[group] = lag
    probe.close()
    return out


def _flink(path: str) -> dict:
    with urllib.request.urlopen(f"{FLINK}{path}", timeout=5) as r:
        return json.load(r)


def _ms(value) -> datetime | None:
    return datetime.fromtimestamp(value / 1000, UTC) if value else None


def _checkpoints() -> list[Check]:
    checks = []
    for job in _flink("/jobs/overview")["jobs"]:
        if job["state"] != "RUNNING":
            continue
        latest = _flink(f"/jobs/{job['jid']}/checkpoints").get("latest", {})
        failed = (latest.get("failed") or {}).get("failure_timestamp")
        done = (latest.get("completed") or {}).get("latest_ack_timestamp")
        alert = checkpoint_alert(_ms(failed), _ms(done))
        value = f"failed at {_ms(failed):%H:%M:%S}Z" if alert else "latest checkpoint completed"
        checks.append(Check(f"checkpoint {job['name']}", alert, value))
    return checks


def _slot_bytes() -> int | None:
    import psycopg

    with psycopg.connect(ORDERS_DSN, connect_timeout=5) as conn:
        row = conn.execute(
            "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint "
            "FROM pg_replication_slots WHERE slot_name = 'ct_cdc_slot'"
        ).fetchone()
    return None if row is None else int(row[0])


def _newest_kafka() -> datetime | None:
    """Record time of the newest message in the GPS topic (the last one of each partition)."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "ct-health-probe",
                         "enable.auto.commit": False})  # fmt: skip
    meta = consumer.list_topics(GPS_TOPIC, timeout=10).topics[GPS_TOPIC]
    newest = None
    for p in meta.partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(GPS_TOPIC, p), timeout=5)
        if high > low:
            consumer.assign([TopicPartition(GPS_TOPIC, p, high - 1)])
            msg = consumer.poll(5)
            if msg is not None and not msg.error():
                ts = datetime.fromtimestamp(msg.timestamp()[1] / 1000, UTC)
                newest = ts if newest is None else max(newest, ts)
    consumer.close()
    return newest


def _newest_bronze() -> datetime | None:
    import trino

    conn = trino.dbapi.connect(host="localhost", port=8080, user="health", catalog="lakehouse")
    cur = conn.cursor()
    cur.execute("SELECT max(ingest_ts) FROM bronze.gps_pings "
                "WHERE ingest_ts > current_timestamp - INTERVAL '1' DAY")  # fmt: skip
    (value,) = cur.fetchone()
    return value


def collect(quick: bool) -> list[Check]:
    checks: list[Check] = []
    first = None if quick else _lags()
    if not quick:
        time.sleep(30)
    second = _lags()
    for group, lag in second.items():
        before = None if first is None else first.get(group)
        trend = f"{lag}" if before is None else f"{before} → {lag}"
        checks.append(Check(f"lag {group}", lag_alert(before, lag), f"{trend} messages"))
    checks += _checkpoints()
    slot = _slot_bytes()
    checks.append(Check("replication slot ct_cdc_slot", slot_alert(slot),
                        "no slot" if slot is None else f"{slot / 1048576:.1f} MB"))  # fmt: skip
    now = datetime.now(UTC)
    kafka, bronze = _newest_kafka(), _newest_bronze()
    value = (
        (
            f"newest GPS message {kafka:%H:%M:%S}Z, newest bronze row "
            f"{'none' if bronze is None else f'{bronze:%H:%M:%S}Z'}"
        )
        if kafka
        else "no GPS messages"
    )
    checks.append(Check("bronze receives rows", bronze_stuck(kafka, bronze, now), value))
    return checks


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--quick", action="store_true", help="one lag sample, no 30 s wait")
    checks = collect(parser.parse_args(argv).quick)
    print("\n".join(report(checks)))
    return exit_code(checks)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

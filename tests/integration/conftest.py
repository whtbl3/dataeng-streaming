"""Helpers for end-to-end tests against the local Docker stack (`make up` first)."""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypeVar

import pytest

from simulator.day import local_dates, merged_stream, simulate_day
from simulator.producer import GpsEncoder, make_producer, produce_all

ROOT = Path(__file__).resolve().parents[2]
PROFILES = os.environ.get("PROFILES", "lake cdc").split()  # as the Makefile (spec 009)
COMPOSE = [
    "docker",
    "compose",
    "-f",
    str(ROOT / "infra/local/docker-compose.yml"),
    *[arg for p in PROFILES for arg in ("--profile", p)],
]
TOPIC = os.environ.get("GPS_TOPIC", "gps.pings.v1")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:18081")
TABLE = "lakehouse.bronze.gps_pings"
CONNECT_URL = "http://localhost:8083"
ORDERS_DSN = os.environ.get("ORDERS_DSN", "postgresql://ct:ct-local@localhost:15432/control_tower")

T = TypeVar("T")


def pg_query(sql: str, params: tuple = ()) -> list[tuple]:
    import psycopg

    with psycopg.connect(ORDERS_DSN) as conn:
        return conn.execute(sql, params).fetchall()


def connector_status() -> dict:
    import urllib.request

    with urllib.request.urlopen(f"{CONNECT_URL}/connectors/ct-orders/status", timeout=5) as r:
        return json.load(r)


def connector_running() -> bool:
    try:
        s = connector_status()
    except OSError:
        return False
    return s["connector"]["state"] == "RUNNING" and [t["state"] for t in s["tasks"]] == ["RUNNING"]


def run_orders_sim(prefix: str, *, vans: int = 4, orders_per_day: int = 20, hours: float = 24):
    """Run the day simulator (orders only) for [now - hours, now]; return DB changes made."""
    from simulator.__main__ import build_parser, run_day
    from simulator.orders_db import connect
    from simulator.producer import make_producer as _mp

    args = build_parser().parse_args(
        [
            "day",
            "--vans",
            str(vans),
            "--orders-per-day",
            str(orders_per_day),
            "--history-hours",
            str(hours),
            "--prefix",
            prefix,
            "--no-gps",
            "--dsn",
            ORDERS_DSN,
        ]
    )
    changes, _, _, _ = run_day(args, _mp, connect)
    return changes


def run_gps_sim(prefix: str, *extra: str, vans: int = 2, hours: float = 1.0, seed: int = 11):
    """Send [now - hours, now] of GPS only, vehicle ids renamed to `prefix-NNNN`.

    `extra` adds CLI flags such as "--chaos". Chaos decisions are made before the renaming, so
    two prefixes with the same seed get the same day, clean or dirty. Returns (pings, chaos).
    """
    from simulator.__main__ import build_parser, run_day
    from simulator.orders_db import connect
    from simulator.producer import make_producer as _mp

    argv = ["day", "--vans", str(vans), "--history-hours", str(hours), "--seed", str(seed)]
    argv += ["--no-db", "--vehicle-prefix", prefix, "--registry", REGISTRY, *extra]
    args = build_parser().parse_args(argv)
    _, pings, _, chaos = run_day(args, _mp, connect, now=SIM_NOW)
    return pings, chaos


# Every GPS simulation in one test session ends at the same instant, so two runs with the same
# seed produce identical pings (spec 004 AC2 compares a clean and a chaos run).
SIM_NOW = datetime.now(UTC).replace(microsecond=0)


def send_ping(ping, sent_at: datetime) -> None:
    """Send one valid ping with an explicit send time (the Kafka record time)."""
    from simulator.producer import send

    producer = make_producer(BOOTSTRAP)
    send(producer, TOPIC, ping, GpsEncoder.from_registry(REGISTRY), sent_at=sent_at)
    assert producer.flush(10) == 0


SILVER_GPS = "lakehouse.silver.gps_pings"


def silver_gps_rows(prefix: str) -> list[tuple]:
    """(van number, event_ts, lat, lon, speed, heading, ingest_ts, lateness_s, is_late)."""
    return query(
        f"SELECT split_part(vehicle_id, '-', 3), event_ts, lat, lon, speed_kmh, heading_deg, "
        f"ingest_ts, lateness_s, is_late FROM {SILVER_GPS} WHERE vehicle_id LIKE ? "
        "ORDER BY 1, 2",
        (f"{prefix}-%",),
    )


def count_table(table: str, prefix: str) -> int:
    return query(f"SELECT count(*) FROM {table} WHERE vehicle_id LIKE ?", (f"{prefix}-%",))[0][0]


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run([*COMPOSE, *args], capture_output=True, text=True, check=check)


def wait_for(fn: Callable[[], T], ok: Callable[[T], bool], timeout_s: float, every_s: float = 2.0):
    """Poll `fn` until `ok(result)`; return (result, seconds waited) or fail."""
    start = time.monotonic()
    while True:
        result = fn()
        elapsed = time.monotonic() - start
        if ok(result):
            return result, elapsed
        if elapsed > timeout_s:
            pytest.fail(f"condition not met after {timeout_s}s; last value: {result!r}")
        time.sleep(every_s)


def query(sql: str, params: tuple = ()) -> list[tuple]:
    import trino

    conn = trino.dbapi.connect(host="localhost", port=8080, user="e2e")
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return [tuple(row) for row in cur.fetchall()]  # trino returns lists
    finally:
        conn.close()


def count_prefix(prefix: str) -> int:
    return query(f"SELECT count(*) FROM {TABLE} WHERE vehicle_id LIKE ?", (f"{prefix}-%",))[0][0]


def produce(prefix: str, *, vans: int, steps: int, interval_s: int = 5, seed: int = 7) -> int:
    """Send `vans * steps` GPS pings ending now, with vehicle ids renamed to `prefix-NNNN`."""
    epoch = int(datetime.now(UTC).timestamp())
    end = datetime.fromtimestamp(epoch - epoch % interval_s, UTC)
    start = end - timedelta(seconds=interval_s * (steps - 1))
    plans = [
        simulate_day(seed=seed, local_date=d, vans=vans, orders_per_day=0, interval_s=interval_s)
        for d in local_dates(start, end)
    ]
    pings = (
        type(p)(
            f"{prefix}-{p.vehicle_id.split('-')[-1]}",
            p.event_ts,
            p.lat,
            p.lon,
            p.speed_kmh,
            p.heading_deg,
        )
        for _, _, p in merged_stream(plans, start, end, orders=False)
    )
    encoder = GpsEncoder.from_registry(REGISTRY)
    return produce_all(make_producer(BOOTSTRAP), TOPIC, pings, encoder)


def compose_services() -> list[dict]:
    out = compose("ps", "-a", "--format", "json").stdout.strip()
    if not out:
        return []
    if out.startswith("["):
        return json.loads(out)
    return [json.loads(line) for line in out.splitlines()]


@pytest.fixture
def prefix() -> str:
    """Unique vehicle-id prefix so each test only counts its own rows."""
    return f"E2E-{uuid.uuid4().hex[:8]}"


def produce_raw(topic: str, key: bytes, value: bytes) -> tuple[int, int]:
    """Send one message as-is and return its (partition, offset) from the delivery report."""
    from confluent_kafka import Producer

    out: dict[str, tuple[int, int]] = {}

    def done(err, msg) -> None:
        if err is None:
            out["at"] = (msg.partition(), msg.offset())

    p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all"})
    p.produce(topic, key=key, value=value, on_delivery=done)
    p.flush(10)
    assert "at" in out, f"message to {topic} was not delivered"
    return out["at"]


def dlq_rows(topic: str, partition: int, offset: int) -> list[tuple]:
    return query(
        "SELECT reason, detail, schema_id, raw_value FROM lakehouse.bronze.dlq_events "
        "WHERE source_topic = ? AND kafka_partition = ? AND kafka_offset = ?",
        (topic, partition, offset),
    )


def registry_get(path: str):
    import urllib.request

    with urllib.request.urlopen(f"{REGISTRY}{path}", timeout=5) as r:
        return json.load(r)


def job_states() -> dict[str, str]:
    import urllib.request

    with urllib.request.urlopen("http://localhost:8081/jobs/overview", timeout=5) as r:
        jobs = json.load(r)["jobs"]
    # One state per job name: RUNNING if any run of it runs (earlier runs stopped with a
    # savepoint stay listed as FINISHED under the same name), else the newest run's state.
    states: dict[str, str] = {}
    for j in sorted(jobs, key=lambda j: j.get("start-time", 0)):
        if j["state"] != "CANCELED" and states.get(j["name"]) != "RUNNING":
            states[j["name"]] = j["state"]
    return states

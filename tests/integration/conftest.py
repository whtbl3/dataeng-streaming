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
from simulator.producer import make_producer, produce_all

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-f", str(ROOT / "infra/local/docker-compose.yml")]
TOPIC = os.environ.get("GPS_TOPIC", "gps.pings.v1")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
TABLE = "lakehouse.bronze.gps_pings"

T = TypeVar("T")


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
    return produce_all(make_producer(BOOTSTRAP), TOPIC, pings)


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

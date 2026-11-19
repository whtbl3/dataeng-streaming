"""Spec 006 acceptance tests for the control tower job, against the running stack (`make up`).

Each test drives one van of its own from a reserved range (VAN-0181…VAN-0200): it writes its
orders to the order database (CDC carries them to the job) and sends that van's pings at the
current time, so the alert horizon (10 min) lets them raise alerts. Teardown closes the orders.
"""

from __future__ import annotations

import io
import itertools
import json
import statistics
import subprocess
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from conftest import BOOTSTRAP, ORDERS_DSN, ROOT, send_ping, wait_for

from flink.udf.eta import route_eta
from simulator.fleet import Ping
from simulator.world import DEPOT_BY_ID, VANS

pytestmark = pytest.mark.e2e

TIMEOUT_S = 60
_POOL = VANS[160:200]  # VAN-0161 … VAN-0200, used by these tests only
_RESERVED = itertools.cycle(_POOL)
ALERT_SCHEMA = json.loads((ROOT / "schemas/alerts.v1-value/v1.avsc").read_text())


def pg(sql: str, params: tuple = ()) -> list[tuple]:
    import psycopg

    with psycopg.connect(ORDERS_DSN, autocommit=True) as conn:
        cur = conn.execute(sql, params)
        return cur.fetchall() if cur.description else []


def now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class ReservedVan:
    """One reserved van: its depot, orders this test creates, and pings sent now.

    A test that sends pings dated in the past passes `pings_from`: it gets a van whose newest
    position is older than that, because a van never moves back to an older ping (AC3)."""

    def __init__(self, pings_from: datetime | None = None) -> None:
        for _ in range(len(_POOL)):
            self.van = next(_RESERVED)
            if pings_from is None:
                break
            rows = pg(
                "SELECT event_ts FROM serving.vehicle_latest WHERE van_id = %s", (self.van.van_id,)
            )
            if not rows or rows[0][0] < pings_from:
                break
        else:
            raise RuntimeError(f"no reserved van without pings after {pings_from}")
        self.depot = DEPOT_BY_ID[self.van.depot_id]
        self.id = self.van.van_id
        self.orders: list[str] = []
        self.tag = uuid.uuid4().hex[:6].upper()
        self.started = pings_from or now()

    def place(self, north_km: float) -> tuple[float, float]:
        return (self.depot.lat + north_km / 111.2, self.depot.lon)

    def order(self, north_km: float, promised_in_min: float, status="out_for_delivery") -> str:
        oid = f"E2E-CT-{self.tag}-{len(self.orders) + 1}"
        t = now()
        dest = self.place(north_km)
        pg(
            "INSERT INTO orders (order_id, depot_id, van_id, stop_seq, dest_lat, dest_lon, "
            "promised_start, promised_end, status, created_at, assigned_at, "
            "out_for_delivery_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s)",
            (oid, self.depot.depot_id, self.id, len(self.orders) + 1, *dest,
             t - timedelta(hours=1), t + timedelta(minutes=promised_in_min), status,
             t - timedelta(hours=2), t - timedelta(hours=1), t, t),
        )  # fmt: skip
        self.orders.append(oid)
        return oid

    def close(self, oid: str, status: str = "delivered") -> datetime:
        t = datetime.now(UTC)
        column = "delivered_at" if status == "delivered" else "failed_at"
        pg(
            f"UPDATE orders SET status = %s, {column} = %s, updated_at = %s WHERE order_id = %s",
            (status, t, t, oid),
        )
        return t

    def ping(self, at: tuple[float, float], speed: float, ts: datetime | None = None) -> datetime:
        ts = ts or now()
        send_ping(Ping(self.id, ts, at[0], at[1], speed, 0.0), ts)
        return ts

    def teardown(self) -> None:
        open_ = pg(
            "SELECT order_id FROM orders WHERE order_id = ANY(%s) "
            "AND status NOT IN ('delivered', 'failed')",
            (self.orders,),
        )
        for (oid,) in open_:
            self.close(oid)


@pytest.fixture
def van():
    v = ReservedVan()
    yield v
    v.teardown()


def eta_row(oid: str):
    rows = pg(
        "SELECT status, eta, promised_end, distance_km, written_at FROM serving.order_eta "
        "WHERE order_id = %s",
        (oid,),
    )
    return rows[0] if rows else None


def alert_rows(van_id: str, kind: str, since: datetime) -> list[tuple]:
    """This van's alerts of one type raised at or after `since` (earlier runs use the same vans)."""
    return pg(
        "SELECT alert_id, order_id, raised_at, resolved_at, raised_written_at FROM serving.alerts "
        "WHERE van_id = %s AND type = %s AND raised_at >= %s ORDER BY raised_at",
        (van_id, kind, since),
    )


def test_open_order_gets_an_eta_within_10s(van):
    """AC2: an ETA by the agreed formula, in the serving database within 10 s of the ping."""
    oid = van.order(north_km=8, promised_in_min=600)
    time.sleep(3)  # let the order change reach the job first (CDC)
    sent = van.ping(van.place(0), 30.0)
    row, _ = wait_for(lambda: eta_row(oid), lambda r: r is not None and r[1] is not None, TIMEOUT_S)
    status, eta, _, km, written = row
    expected, expected_km = route_eta(sent, van.place(0), 30.0, [van.place(8)])
    assert status == "out_for_delivery"
    assert abs((eta - expected).total_seconds()) < 60 and km == pytest.approx(expected_km, 0.01)
    assert (written - sent).total_seconds() < 10


def test_late_ping_does_not_move_the_van_back(van):
    """AC3."""
    newest = van.ping(van.place(2), 30.0)
    van.ping(van.place(5), 30.0, ts=newest - timedelta(seconds=30))  # older, arrives later
    time.sleep(2)
    van.ping(van.place(6), 30.0, ts=newest - timedelta(seconds=20))

    def latest():
        rows = pg("SELECT event_ts, lat FROM serving.vehicle_latest WHERE van_id = %s", (van.id,))
        return rows[0] if rows else None

    row, _ = wait_for(latest, lambda r: r is not None and r[0] == newest, TIMEOUT_S)
    time.sleep(5)
    assert latest() == row and row[1] == pytest.approx(van.place(2)[0])


def test_sla_risk_raised_once_and_resolved_on_delivery(van):
    """AC4: 8 km away at 30 km/h is 16 min; promised in 5 min → one alert; delivery resolves it."""
    oid = van.order(north_km=8, promised_in_min=5)
    time.sleep(3)
    for _ in range(5):
        van.ping(van.place(0), 30.0)
        time.sleep(1.1)
    rows, _ = wait_for(
        lambda: alert_rows(van.id, "SLA_RISK", since=van.started), lambda r: len(r) >= 1, TIMEOUT_S
    )
    time.sleep(3)
    (alert,) = alert_rows(van.id, "SLA_RISK", since=van.started)
    assert alert[1] == oid and alert[3] is None
    van.close(oid)
    resolved, _ = wait_for(
        lambda: alert_rows(van.id, "SLA_RISK", since=van.started),
        lambda r: r[0][3] is not None,
        TIMEOUT_S,
    )
    assert len(resolved) == 1
    wait_for(lambda: eta_row(oid), lambda r: r is None, TIMEOUT_S)  # closed orders leave


def test_abnormal_stop_alert():
    """AC6: 11 min still, 3 km from the depot and with no open stop → one alert; moving resolves."""
    t0 = now() - timedelta(minutes=11)
    van = ReservedVan(pings_from=t0)
    try:
        spot = van.place(3)
        for i in range(0, 11 * 60 + 1, 5):  # one ping every 5 s; the last ones are fresh
            van.ping(spot, 0.0, ts=t0 + timedelta(seconds=i))
        rows, _ = wait_for(
            lambda: alert_rows(van.id, "ABNORMAL_STOP", since=t0), lambda r: len(r) >= 1, TIMEOUT_S
        )
        (alert,) = rows
        assert alert[2] == t0 + timedelta(minutes=10)  # raised by the ping at the 10 min mark
        van.ping(van.place(3.1), 25.0, ts=t0 + timedelta(minutes=11, seconds=5))
        wait_for(
            lambda: alert_rows(van.id, "ABNORMAL_STOP", since=t0),
            lambda r: r[0][3] is not None,
            TIMEOUT_S,
        )
    finally:
        van.teardown()


def _alert_messages(van_id: str) -> list[dict]:
    from confluent_kafka import Consumer, TopicPartition
    from fastavro import parse_schema, schemaless_reader

    from contracts.wire import unframe

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP, "group.id": f"e2e-{uuid.uuid4().hex[:8]}",
        "isolation.level": "read_committed", "enable.auto.commit": False,
    })  # fmt: skip
    meta = consumer.list_topics("alerts.v1", timeout=10).topics["alerts.v1"]
    consumer.assign([TopicPartition("alerts.v1", p, 0) for p in meta.partitions])
    schema, out, idle = parse_schema(ALERT_SCHEMA), [], 0
    while idle < 5:
        msg = consumer.poll(1.0)
        if msg is None:
            idle += 1
            continue
        idle = 0
        if msg.error():
            continue
        _, body = unframe(msg.value())
        record = schemaless_reader(io.BytesIO(body), schema)
        if record["van_id"] == van_id:
            out.append(record)
    consumer.close()
    return out


def test_each_alert_event_is_published_once_to_the_alerts_topic(van):
    """AC10: a raise and a resolve, each exactly once, registry-framed Avro (read_committed)."""
    oid = van.order(north_km=8, promised_in_min=5)
    time.sleep(3)
    van.ping(van.place(0), 30.0)
    rows, _ = wait_for(
        lambda: alert_rows(van.id, "SLA_RISK", since=van.started), lambda r: len(r) == 1, TIMEOUT_S
    )
    van.close(oid)
    wait_for(
        lambda: alert_rows(van.id, "SLA_RISK", since=van.started),
        lambda r: r[0][3] is not None,
        TIMEOUT_S,
    )
    # exactly-once Kafka sinks commit with the checkpoint (30 s)
    messages, _ = wait_for(lambda: _alert_messages(van.id), lambda m: len(m) >= 2, 120, every_s=10)
    events = sorted((m["alert_id"], m["event"]) for m in messages)
    assert events == [(rows[0][0], "raised"), (rows[0][0], "resolved")]
    assert all(m["order_id"] == oid and m["type"] == "SLA_RISK" for m in messages)


def test_alert_latency_p95_under_10s():
    """AC7 (NFR-1): ping → alert row in Postgres, p95 < 10 s, over 10 raises on 10 vans."""
    vans = [ReservedVan() for _ in range(10)]
    try:
        for v in vans:
            v.order(north_km=8, promised_in_min=5)
        time.sleep(5)
        sent = {}
        for v in vans:
            sent[v.id] = v.ping(v.place(0), 30.0)
            time.sleep(0.5)
        lags = []
        for v in vans:
            rows, _ = wait_for(
                lambda v=v: alert_rows(v.id, "SLA_RISK", since=v.started),
                lambda r: len(r) >= 1,
                TIMEOUT_S,
            )
            lags.append((rows[-1][4] - sent[v.id]).total_seconds())
        p95 = statistics.quantiles(lags, n=20)[-1]
        print(f"\nalert latency: median {statistics.median(lags):.1f} s, p95 {p95:.1f} s")
        assert p95 < 10
    finally:
        for v in vans:
            v.teardown()


def test_tower_status_matches_the_serving_tables(van):
    """AC11."""
    van.order(north_km=4, promised_in_min=600)
    time.sleep(3)
    van.ping(van.place(0), 30.0)
    wait_for(
        lambda: eta_row(van.orders[0]), lambda r: r is not None and r[1] is not None, TIMEOUT_S
    )
    out = subprocess.run(
        ["make", "-s", "tower-status"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout
    (with_eta,) = pg("SELECT count(*) FROM serving.order_eta WHERE eta IS NOT NULL")[0]
    (open_sla,) = pg(
        "SELECT count(*) FROM serving.alerts WHERE type = 'SLA_RISK' AND resolved_at IS NULL"
    )[0]
    assert f"open orders with an ETA | {with_eta}" in out
    assert f"open SLA_RISK alerts | {open_sla}" in out
    assert van.id in out  # newest position per van

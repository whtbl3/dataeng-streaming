"""Producer encoding, order DB writes and CLI wiring, with fakes (no Docker)."""

import json
from datetime import UTC, date, datetime
from pathlib import Path

from contracts.decode import decode_avro, decode_gps
from contracts.wire import unframe
from simulator.__main__ import main
from simulator.day import OrderEvent, simulate_day
from simulator.fleet import Ping
from simulator.orders_db import OrdersWriter, statement_for
from simulator.producer import GpsEncoder, produce_all

PING = Ping(
    "VAN-0001", datetime(2026, 11, 18, 3, 0, 5, 123000, tzinfo=UTC), 10.7, 106.6, 32.5, 90.0
)
NOW = datetime(2026, 11, 18, 5, 0, 0, tzinfo=UTC)  # 12:00 local
V1 = json.loads(Path("schemas/gps.pings.v1-value/v1.avsc").read_text())
V2 = json.loads(Path("schemas/gps.pings.v1-value/v2.avsc").read_text())
ENC = GpsEncoder(V2, 2)


class Reg:
    def schema_by_id(self, schema_id):
        return {1: V1, 2: V2}[schema_id]


class FakeProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.flushed = False

    def produce(self, topic: str, key: bytes, value: bytes) -> None:
        self.messages.append((topic, key, value))

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float | None = None) -> int:
        self.flushed = True
        return 0


class FakeResult:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount


class FakeDB:
    """Mimics the idempotent SQL: each (order, kind) changes a row only once."""

    def __init__(self) -> None:
        self.applied: list[tuple[str, dict]] = []
        self.seen: set[tuple[str, str]] = set()

    def execute(self, query: str, params: dict) -> FakeResult:
        kind = "created" if query.lstrip().startswith("INSERT") else query.split("'")[1]
        key = (params["order_id"], kind)
        if key in self.seen:
            return FakeResult(0)
        self.seen.add(key)
        self.applied.append((kind, params))
        return FakeResult(1)


def test_encode_keys_by_vehicle_and_writes_registry_framed_avro():
    key, value = ENC.encode(PING)
    assert key == b"VAN-0001"
    assert unframe(value)[0] == 2
    record, err = decode_avro(value, Reg())
    assert err is None
    assert record == {
        "vehicle_id": "VAN-0001",
        "event_ts": datetime(2026, 11, 18, 3, 0, 5, 123000, tzinfo=UTC),
        "lat": 10.7,
        "lon": 106.6,
        "speed_kmh": 32.5,
        "heading_deg": 90.0,
        "battery_pct": record["battery_pct"],
    }
    assert 5 <= record["battery_pct"] <= 100


def test_v1_encoder_writes_only_v1_fields():
    record, _ = decode_avro(GpsEncoder(V1, 1).encode(PING)[1], Reg())
    assert set(record) == {f["name"] for f in V1["fields"]}


def test_avro_smaller_than_json():
    _, avro = GpsEncoder(V1, 1).encode(PING)
    json_size = len(json.dumps(PING.to_dict(), separators=(",", ":")).encode())
    assert len(avro) < json_size / 2


def test_produce_all_sends_every_ping_and_flushes():
    fake = FakeProducer()
    plan = simulate_day(seed=1, local_date=date(2026, 11, 18), vans=3, orders_per_day=6)
    pings = plan.pings(start=plan.start, end=plan.start.replace(minute=0, second=15))
    assert produce_all(fake, "gps.pings.v1", pings, ENC) == 3 * 4
    assert fake.flushed
    assert {t for t, _, _ in fake.messages} == {"gps.pings.v1"}


def test_update_statements_require_the_previous_status():
    ts = datetime(2026, 11, 18, tzinfo=UTC)
    sql, params = statement_for(OrderEvent(ts, "delivered", "ORD-1"))
    assert "status = %(prev)s" in sql and params["prev"] == "out_for_delivery"
    sql, params = statement_for(OrderEvent(ts, "assigned", "ORD-1", {"van_id": "V", "stop_seq": 2}))
    assert params["prev"] == "created" and params["van_id"] == "V"
    sql, _ = statement_for(OrderEvent(ts, "created", "ORD-1", {"depot_id": "DEP-01"}))
    assert "ON CONFLICT (order_id) DO NOTHING" in sql


def test_writer_counts_only_real_changes():
    db = FakeDB()
    writer = OrdersWriter(db)
    ev = OrderEvent(NOW, "out_for_delivery", "ORD-1")
    assert writer.apply(ev) == 1
    assert writer.apply(ev) == 0  # re-run is a no-op
    assert writer.changes == 1


def test_cli_history_mode_sends_window_pings_and_orders_up_to_now(capsys):
    producer, db = FakeProducer(), FakeDB()
    argv = ["day", "--vans", "2", "--orders-per-day", "8", "--history-hours", "1", "--seed", "3"]
    assert (
        main(argv, lambda _b: producer, lambda _d: db, now=NOW, encoder_factory=lambda _a: ENC) == 0
    )
    out = capsys.readouterr().out
    assert len(producer.messages) == 2 * 721  # 1 h window, both ends inclusive, every 5 s
    assert all(p["ts"] <= NOW for _, p in db.applied)
    assert f"order_changes={len(db.applied)} pings=1442" in out
    # re-running the same window writes nothing new
    main(argv, lambda _b: FakeProducer(), lambda _d: db, now=NOW, encoder_factory=lambda _a: ENC)
    assert "order_changes=0" in capsys.readouterr().out


def test_cli_invalid_rate_reports_labelled_counts(capsys):
    producer = FakeProducer()
    argv = [
        "day",
        "--vans",
        "2",
        "--orders-per-day",
        "8",
        "--history-hours",
        "1",
        "--no-db",
        "--invalid-rate",
        "0.1",
    ]
    main(argv, lambda _b: producer, lambda _d: FakeDB(), now=NOW, encoder_factory=lambda _a: ENC)
    out = capsys.readouterr().out
    assert "invalid=" in out and "not_decodable" in out
    ingest = datetime.now(UTC)  # future-time pings are relative to the wall clock
    bad = sum(1 for _, _, v in producer.messages if decode_gps(v, ingest, Reg())[1])
    assert 0.05 * 1442 < bad < 0.15 * 1442


def test_cli_seed_sql(capsys):
    assert main(["seed-sql"]) == 0
    assert "INSERT INTO depots" in capsys.readouterr().out

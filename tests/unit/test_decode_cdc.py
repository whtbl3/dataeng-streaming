"""Decoding Debezium Avro change events (spec 003 AC1-AC3 for CDC). Schema captured from the
registry in T7 (`tests/fixtures/cdc_orders_value.avsc`)."""

import io
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastavro import parse_schema, schemaless_writer

from contracts.cdc import TABLES, decode_cdc
from contracts.registry import UnknownSchemaId
from contracts.wire import frame

ORDERS = json.loads(Path("tests/fixtures/cdc_orders_value.avsc").read_text())


class Reg:
    def __init__(self, schemas):
        self.schemas = schemas

    def schema_by_id(self, schema_id):
        if schema_id not in self.schemas:  # same contract as contracts.registry.Registry
            raise UnknownSchemaId(schema_id)
        return self.schemas[schema_id]


ROW = {
    "order_id": "ORD-20261118-00001",
    "depot_id": "DEP-01",
    "van_id": "VAN-0001",
    "stop_seq": 1,
    "dest_lat": 10.790626,
    "dest_lon": 106.681708,
    "promised_start": "2026-11-18T00:36:11Z",
    "promised_end": "2026-11-18T02:36:11Z",
    "status": "out_for_delivery",
    "failure_reason": None,
    "created_at": "2026-11-17T19:57:55Z",
    "assigned_at": "2026-11-18T00:30:00Z",
    "out_for_delivery_at": "2026-11-18T01:00:00.123456Z",
    "delivered_at": None,
    "failed_at": None,
    "updated_at": "2026-11-18T01:00:00Z",
}
SOURCE = {
    "version": "3.6.3.Final",
    "connector": "postgresql",
    "name": "cdc",
    "ts_ms": 1795145135398,
    "snapshot": "false",
    "db": "control_tower",
    "sequence": None,
    "ts_us": None,
    "ts_ns": None,
    "schema": "public",
    "table": "orders",
    "txId": 1572,
    "lsn": 27390824,
    "xmin": None,
    "origin": None,
    "origin_lsn": None,
}


def envelope(op="u", before=None, after=ROW):
    return {
        "before": before,
        "after": after,
        "source": SOURCE,
        "transaction": None,
        "op": op,
        "ts_ms": 1795145135747,
        "ts_us": None,
        "ts_ns": None,
    }


def encode(record, schema=ORDERS, schema_id=3):
    buf = io.BytesIO()
    schemaless_writer(buf, parse_schema(schema), record)
    return frame(schema_id, buf.getvalue())


def test_update_decodes_to_the_bronze_changelog_fields():
    before = {**ROW, "status": "assigned", "out_for_delivery_at": None}
    row, err = decode_cdc(encode(envelope("u", before, ROW)), "orders", Reg({3: ORDERS}))
    assert err is None
    assert row["op"] == "u" and row["lsn"] == 27390824 and row["tx_id"] == 1572
    assert row["snapshot"] == "false"
    assert row["commit_ts"] == datetime(2026, 11, 20, 3, 25, 35, 398000, tzinfo=UTC)
    assert row["capture_ts"] == datetime(2026, 11, 20, 3, 25, 35, 747000, tzinfo=UTC)
    assert row["before"]["status"] == "assigned" and row["before"]["out_for_delivery_at"] is None
    after = row["after"]
    assert after["out_for_delivery_at"] == datetime(2026, 11, 18, 1, 0, 0, 123456, tzinfo=UTC)
    assert after["promised_end"] == datetime(2026, 11, 18, 2, 36, 11, tzinfo=UTC)
    assert list(after) == [name for name, _ in TABLES["orders"]]


def test_create_has_no_before_image():
    row, err = decode_cdc(encode(envelope("c", None, ROW)), "orders", Reg({3: ORDERS}))
    assert err is None and row["op"] == "c" and row["before"] is None


def test_a_new_nullable_column_from_the_database_is_ignored():
    evolved = json.loads(json.dumps(ORDERS))
    value = evolved["fields"][0]["type"][1]
    value["fields"].append({"name": "note", "type": ["null", "string"], "default": None})
    rec = envelope("u", None, {**ROW, "note": "leave at door"})
    row, err = decode_cdc(encode(rec, evolved, 9), "orders", Reg({9: evolved}))
    assert err is None and "note" not in row["after"]


@pytest.mark.parametrize(
    "raw, reason",
    [
        (b'{"op":"u"}', "not_decodable"),
        (frame(42, b"\x00"), "unknown_schema_id"),
    ],
)
def test_bad_cdc_messages_get_a_reason(raw, reason):
    row, err = decode_cdc(raw, "orders", Reg({3: ORDERS}))
    assert row is None and err[0] == reason


def test_a_message_without_the_debezium_envelope_is_schema_mismatch():
    other = {"type": "record", "name": "X", "fields": [{"name": "a", "type": "int"}]}
    row, err = decode_cdc(encode({"a": 1}, other, 7), "orders", Reg({7: other}))
    assert row is None and err[0] == "schema_mismatch"

"""Decoding registry-framed Avro GPS pings, with a fake registry (spec 003 AC3, AC4, AC6)."""

import io
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastavro import parse_schema, schemaless_writer

from contracts.decode import decode_gps
from contracts.registry import RegistryUnavailable, UnknownSchemaId
from contracts.wire import frame

V1 = json.loads(Path("schemas/gps.pings.v1-value/v1.avsc").read_text())
V2 = json.loads(Path("schemas/gps.pings.v1-value/v2.avsc").read_text())
INGEST = datetime(2026, 11, 18, 1, 2, 10, tzinfo=UTC)
PING = {
    "vehicle_id": "VAN-0001",
    "event_ts": datetime(2026, 11, 18, 1, 2, 5, tzinfo=UTC),
    "lat": 10.783058,
    "lon": 106.691112,
    "speed_kmh": 37.5,
    "heading_deg": 309.1,
}


class FakeRegistry:
    def __init__(self, schemas: dict[int, dict], down: bool = False) -> None:
        self.schemas, self.down, self.calls = schemas, down, 0

    def schema_by_id(self, schema_id: int) -> dict:
        self.calls += 1
        if self.down:
            raise RegistryUnavailable("connection refused")
        if schema_id not in self.schemas:
            raise UnknownSchemaId(schema_id)
        return self.schemas[schema_id]


def avro(schema: dict, record: dict) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, parse_schema(schema), record)
    return buf.getvalue()


REG = FakeRegistry({1: V1, 2: V2})


def test_valid_v1_ping_decodes_to_the_bronze_fields():
    row, err = decode_gps(frame(1, avro(V1, PING)), INGEST, REG)
    assert err is None and row == PING


def test_v2_writer_v1_reader_ignores_the_new_field():
    row, err = decode_gps(frame(2, avro(V2, {**PING, "battery_pct": 81})), INGEST, REG)
    assert err is None and row == PING and "battery_pct" not in row


def test_writer_schemas_are_cached():
    reg = FakeRegistry({1: V1})
    for _ in range(3):
        decode_gps(frame(1, avro(V1, PING)), INGEST, reg)
    assert reg.calls == 1


@pytest.mark.parametrize(
    "raw, reason",
    [
        (b'{"vehicle_id":"VAN-0001"}', "not_decodable"),
        (b"", "not_decodable"),
        (frame(1, avro(V1, PING))[:12], "not_decodable"),  # truncated body
        (frame(1, avro(V1, PING)) + b"\x00\x01", "not_decodable"),  # trailing bytes
        (frame(99, avro(V1, PING)), "unknown_schema_id"),
    ],
)
def test_undecodable_messages_get_a_reason_instead_of_raising(raw, reason):
    row, err = decode_gps(raw, INGEST, REG)
    assert row is None and err[0] == reason and err[1]


def test_writer_schema_the_reader_cannot_resolve_is_schema_mismatch():
    other = {
        "type": "record",
        "name": "GpsPing",
        "namespace": "ct.gps",
        "fields": [{"name": "vehicle_id", "type": "string"}],
    }
    row, err = decode_gps(
        frame(5, avro(other, {"vehicle_id": "VAN-0001"})), INGEST, FakeRegistry({5: other})
    )
    assert row is None and err[0] == "schema_mismatch" and err[2] == 5


def test_rule_breaks_are_reported_after_decoding():
    raw = frame(1, avro(V1, {**PING, "speed_kmh": 150.0}))
    row, err = decode_gps(raw, INGEST, REG)
    assert row is None and err[0] == "speed_out_of_range" and err[2] == 1
    future = frame(1, avro(V1, {**PING, "event_ts": INGEST + timedelta(minutes=10)}))
    assert decode_gps(future, INGEST, REG)[1][0] == "event_ts_in_future"


def test_registry_outage_is_raised_not_dead_lettered():
    with pytest.raises(RegistryUnavailable):
        decode_gps(frame(1, avro(V1, PING)), INGEST, FakeRegistry({}, down=True))

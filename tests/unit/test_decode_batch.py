"""Decoding a batch gives exactly what decoding row by row gives (spec 005 AC3)."""

from datetime import timedelta

from test_decode import INGEST, PING, REG, V1, V2, avro

from contracts.decode import (
    GPS_FIELDS,
    GPS_RESULT_FIELDS,
    gps_result,
    gps_result_columns,
)
from contracts.wire import frame

MIXED = [
    frame(1, avro(V1, PING)),
    b"not avro at all",
    frame(2, avro(V2, {**PING, "vehicle_id": "VAN-0002", "battery_pct": 81})),
    frame(1, avro(V1, {**PING, "speed_kmh": 150.0})),
    None,
    frame(99, avro(V1, PING)),
    frame(1, avro(V1, {**PING, "event_ts": INGEST + timedelta(minutes=10)})),
    frame(1, avro(V1, {**PING, "vehicle_id": "VAN-0003"})),
]
INGESTS = [INGEST + timedelta(seconds=i) for i in range(len(MIXED))]


def test_result_has_the_ping_fields_then_the_error_fields():
    assert [
        *GPS_FIELDS,
        "error_reason",
        "error_detail",
        "schema_id",
        "raw_value",
    ] == GPS_RESULT_FIELDS


def test_good_ping_has_no_error_and_no_raw_bytes():
    out = dict(zip(GPS_RESULT_FIELDS, gps_result(MIXED[0], INGEST, REG), strict=True))
    assert out["vehicle_id"] == "VAN-0001" and out["error_reason"] is None
    assert out["raw_value"] is None


def test_bad_message_keeps_its_raw_bytes_and_no_ping_fields():
    out = dict(zip(GPS_RESULT_FIELDS, gps_result(MIXED[1], INGEST, REG), strict=True))
    assert out["error_reason"] == "not_decodable" and out["raw_value"] == MIXED[1]
    assert all(out[f] is None for f in GPS_FIELDS)


def test_batch_columns_equal_the_row_by_row_results():
    columns = gps_result_columns(MIXED, INGESTS, REG)
    assert list(columns) == GPS_RESULT_FIELDS
    rows = [gps_result(raw, ts, REG) for raw, ts in zip(MIXED, INGESTS, strict=True)]
    assert list(zip(*columns.values(), strict=True)) == rows
    reasons = columns["error_reason"]
    assert reasons == [
        None,
        "not_decodable",
        None,
        "speed_out_of_range",
        "not_decodable",
        "unknown_schema_id",
        "event_ts_in_future",
        None,
    ]


def test_empty_batch_gives_empty_columns():
    assert gps_result_columns([], [], REG) == {f: [] for f in GPS_RESULT_FIELDS}

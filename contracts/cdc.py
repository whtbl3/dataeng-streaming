"""Decode Debezium Avro change events into the bronze changelog shape (specs 002 and 003).

The writer schema comes from the registry (Debezium registers it from the table definition).
Only the columns bronze stores are kept, in bronze order, so a new nullable column in the
database never breaks the job. Debezium's `ZonedTimestamp` strings become UTC datetimes.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from contracts.decode import Result, SchemaSource, decode_avro
from contracts.wire import unframe

# Bronze columns per table: (name, Flink type). Timestamps are TIMESTAMP_LTZ(6) in bronze.
TS = "TIMESTAMP_LTZ(6)"
TABLES: dict[str, list[tuple[str, str]]] = {
    "depots": [
        ("depot_id", "STRING"), ("name", "STRING"), ("lat", "DOUBLE"), ("lon", "DOUBLE"),
        ("created_at", TS), ("updated_at", TS),
    ],
    "vans": [
        ("van_id", "STRING"), ("depot_id", "STRING"), ("plate", "STRING"), ("capacity", "INT"),
        ("created_at", TS), ("updated_at", TS),
    ],
    "orders": [
        ("order_id", "STRING"), ("depot_id", "STRING"), ("van_id", "STRING"),
        ("stop_seq", "INT"), ("dest_lat", "DOUBLE"), ("dest_lon", "DOUBLE"),
        ("promised_start", TS), ("promised_end", TS), ("status", "STRING"),
        ("failure_reason", "STRING"), ("created_at", TS), ("assigned_at", TS),
        ("out_for_delivery_at", TS), ("delivered_at", TS), ("failed_at", TS),
        ("updated_at", TS),
    ],
}  # fmt: skip


def _ts(v: Any) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.astimezone(UTC)
    return datetime.fromisoformat(str(v)).astimezone(UTC)


def _ms(v: int | None) -> datetime | None:
    return None if v is None else datetime.fromtimestamp(v / 1000, UTC)


def _image(rec: dict | None, table: str) -> dict | None:
    if rec is None:
        return None
    return {
        name: _ts(rec.get(name)) if ftype == TS else rec.get(name) for name, ftype in TABLES[table]
    }


def decode_cdc(raw: bytes | None, table: str, registry: SchemaSource) -> Result:
    record, err = decode_avro(raw, registry)
    if err:
        return None, err
    if (
        not isinstance(record, dict)
        or "op" not in record
        or not isinstance(record.get("source"), dict)
    ):
        return None, ("schema_mismatch", "not a Debezium change event envelope", unframe(raw)[0])
    src = record["source"]
    try:
        row = {
            "op": record["op"],
            "before": _image(record.get("before"), table),
            "after": _image(record.get("after"), table),
            "snapshot": src.get("snapshot"),
            "lsn": src.get("lsn"),
            "tx_id": src.get("txId"),
            "commit_ts": _ms(src.get("ts_ms")),
            "capture_ts": _ms(record.get("ts_ms")),
        }
    except (TypeError, ValueError) as exc:
        return None, ("schema_mismatch", f"{type(exc).__name__}: {exc}"[:500], unframe(raw)[0])
    return row, None

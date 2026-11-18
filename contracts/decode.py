"""Decode registry-framed Avro messages and apply the rules (spec 003).

Each function returns `(row, None)` for a good message or `(None, error)` for a bad one, where
`error = (reason, detail, schema_id)`. It never raises for a bad *message*; it raises only when
the registry itself is down (see `contracts.registry`).
"""

from __future__ import annotations

import io
import json
import os
from datetime import datetime
from functools import cache
from pathlib import Path
from typing import Any, Protocol
from weakref import WeakKeyDictionary

from fastavro import parse_schema, schemaless_reader
from fastavro.read import SchemaResolutionError

from contracts.registry import UnknownSchemaId
from contracts.validate import gps_violation
from contracts.wire import WireError, unframe

Error = tuple[str, str, int | None]
Result = tuple[dict[str, Any] | None, Error | None]

SCHEMAS_DIR = Path(
    os.environ.get("CT_SCHEMAS_DIR", Path(__file__).resolve().parents[1] / "schemas")
)
GPS_SUBJECT = "gps.pings.v1-value"
GPS_READER_VERSION = "v1"  # what bronze stores; newer writer versions resolve against it


class SchemaSource(Protocol):
    def schema_by_id(self, schema_id: int) -> dict: ...


@cache
def reader_schema(subject: str, version: str) -> dict:
    return json.loads((SCHEMAS_DIR / subject / f"{version}.avsc").read_text())


# Parsed writer schemas, cached per registry (a schema id is only unique within one registry).
_writers: WeakKeyDictionary[Any, dict[int, Any]] = WeakKeyDictionary()
_readers: dict[str, Any] = {}


def _parsed_writer(schema_id: int, registry: SchemaSource) -> Any:
    cache_ = _writers.setdefault(registry, {})
    if schema_id not in cache_:
        cache_[schema_id] = parse_schema(registry.schema_by_id(schema_id), _write_hint=False)
    return cache_[schema_id]


def _parsed_reader(schema: dict) -> Any:
    key = json.dumps(schema, sort_keys=True)
    if key not in _readers:
        _readers[key] = parse_schema(schema, _write_hint=False)
    return _readers[key]


def decode_avro(raw: bytes | None, registry: SchemaSource, reader: dict | None = None) -> Result:
    """Wire format + writer schema from the registry (+ optional reader schema)."""
    try:
        schema_id, body = unframe(raw)
    except WireError as exc:
        return None, ("not_decodable", str(exc), None)
    try:
        writer = _parsed_writer(schema_id, registry)
    except UnknownSchemaId as exc:
        return None, ("unknown_schema_id", str(exc), schema_id)
    buf = io.BytesIO(body)
    try:
        record = schemaless_reader(buf, writer, _parsed_reader(reader) if reader else None)
    except SchemaResolutionError as exc:
        return None, ("schema_mismatch", str(exc)[:500], schema_id)
    except Exception as exc:  # noqa: BLE001 - any failure to read the body is "not decodable"
        return None, ("not_decodable", f"{type(exc).__name__}: {exc}"[:500], schema_id)
    if buf.tell() != len(body):
        return None, ("not_decodable", f"{len(body) - buf.tell()} trailing bytes", schema_id)
    return record, None


def decode_gps(raw: bytes | None, ingest_ts: datetime, registry: SchemaSource) -> Result:
    row, err = decode_avro(raw, registry, reader_schema(GPS_SUBJECT, GPS_READER_VERSION))
    if err:
        return None, err
    broken = gps_violation(row, ingest_ts)
    if broken:
        schema_id = unframe(raw)[0]
        return None, (broken[0], broken[1], schema_id)
    return row, None

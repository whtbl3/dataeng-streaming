"""PyFlink scalar UDFs used by the bronze SQL jobs (ADR 0010). The only module importing pyflink.

A UDF takes the raw Kafka value *and* its metadata, and returns one ROW with everything the
sinks need: the decoded fields, `error_reason` / `error_detail` / `schema_id`, the Kafka
metadata, and `raw_value` (only for rejects). The statement set reads nothing but that ROW, so
all sinks share one identical expression and the planner decodes each message once, not once
per sink. The registry URL is an argument, so the SQL file holds the configuration.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pyflink.common import Row
from pyflink.table import DataTypes
from pyflink.table.udf import ScalarFunction, udf

from contracts.decode import decode_gps
from contracts.registry import Registry

TAIL_FIELDS = [
    DataTypes.FIELD("error_reason", DataTypes.STRING()),
    DataTypes.FIELD("error_detail", DataTypes.STRING()),
    DataTypes.FIELD("schema_id", DataTypes.INT()),
    DataTypes.FIELD("raw_value", DataTypes.BYTES()),
    DataTypes.FIELD("ingest_ts", DataTypes.TIMESTAMP_LTZ(3)),
    DataTypes.FIELD("kafka_partition", DataTypes.INT()),
    DataTypes.FIELD("kafka_offset", DataTypes.BIGINT()),
]
GPS_FIELDS = ["vehicle_id", "event_ts", "lat", "lon", "speed_kmh", "heading_deg"]
GPS_RESULT = DataTypes.ROW(
    [
        DataTypes.FIELD("vehicle_id", DataTypes.STRING()),
        DataTypes.FIELD("event_ts", DataTypes.TIMESTAMP_LTZ(3)),
        DataTypes.FIELD("lat", DataTypes.DOUBLE()),
        DataTypes.FIELD("lon", DataTypes.DOUBLE()),
        DataTypes.FIELD("speed_kmh", DataTypes.DOUBLE()),
        DataTypes.FIELD("heading_deg", DataTypes.DOUBLE()),
        *TAIL_FIELDS,
    ]
)


def _utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


class _Registries:
    def __init__(self) -> None:
        self._by_url: dict[str, Registry] = {}

    def get(self, url: str) -> Registry:
        if url not in self._by_url:
            self._by_url[url] = Registry(url)
        return self._by_url[url]


class DecodeGps(ScalarFunction):
    def open(self, function_context) -> None:
        self.registries = _Registries()

    def eval(
        self, raw: bytes, ingest_ts: datetime, partition: int, offset: int, registry_url: str
    ) -> Row:
        row, err = decode_gps(raw, _utc(ingest_ts), self.registries.get(registry_url))
        meta = (ingest_ts, partition, offset)
        if err:
            return Row(*([None] * len(GPS_FIELDS)), err[0], err[1], err[2], raw, *meta)
        return Row(*(row[f] for f in GPS_FIELDS), None, None, None, None, *meta)


decode_gps_udf = udf(DecodeGps(), result_type=GPS_RESULT)

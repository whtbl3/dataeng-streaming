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

from contracts.cdc import TABLES, TS, decode_cdc
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


# ---- CDC (Debezium) -------------------------------------------------------------------
_FLINK_TYPES = {
    "STRING": DataTypes.STRING,
    "DOUBLE": DataTypes.DOUBLE,
    "INT": DataTypes.INT,
    TS: lambda: DataTypes.TIMESTAMP_LTZ(6),
}
CDC_FIELDS = ["op", "before", "after", "snapshot", "lsn", "tx_id", "commit_ts", "capture_ts"]


def _image_type(table: str):
    return DataTypes.ROW([DataTypes.FIELD(n, _FLINK_TYPES[t]()) for n, t in TABLES[table]])


def cdc_result(table: str):
    return DataTypes.ROW(
        [
            DataTypes.FIELD("op", DataTypes.STRING()),
            DataTypes.FIELD("before", _image_type(table)),
            DataTypes.FIELD("after", _image_type(table)),
            DataTypes.FIELD("snapshot", DataTypes.STRING()),
            DataTypes.FIELD("lsn", DataTypes.BIGINT()),
            DataTypes.FIELD("tx_id", DataTypes.BIGINT()),
            DataTypes.FIELD("commit_ts", DataTypes.TIMESTAMP_LTZ(3)),
            DataTypes.FIELD("capture_ts", DataTypes.TIMESTAMP_LTZ(3)),
            *TAIL_FIELDS,
        ]
    )


class DecodeCdc(ScalarFunction):
    def __init__(self, table: str) -> None:
        self.table = table

    def open(self, function_context) -> None:
        self.registries = _Registries()

    def eval(
        self, raw: bytes, ingest_ts: datetime, partition: int, offset: int, registry_url: str
    ) -> Row:
        row, err = decode_cdc(raw, self.table, self.registries.get(registry_url))
        meta = (ingest_ts, partition, offset)
        if err:
            return Row(*([None] * len(CDC_FIELDS)), err[0], err[1], err[2], raw, *meta)
        image = lambda d: None if d is None else Row(*d.values())  # noqa: E731
        values = [image(row[f]) if f in ("before", "after") else row[f] for f in CDC_FIELDS]
        return Row(*values, None, None, None, None, *meta)


decode_depots_cdc_udf = udf(DecodeCdc("depots"), result_type=cdc_result("depots"))
decode_vans_cdc_udf = udf(DecodeCdc("vans"), result_type=cdc_result("vans"))
decode_orders_cdc_udf = udf(DecodeCdc("orders"), result_type=cdc_result("orders"))

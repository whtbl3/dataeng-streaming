"""control_tower: live ETA, SLA-risk and abnormal-stop alerts (spec 006, ADR 0013).

Kafka GPS + order changes → one state per van (`flink.udf.tower`) → Postgres serving tables and
the alerts.v1 topic. Reads Kafka directly, not silver: the alert path must react within seconds.
Bad GPS messages are skipped here; bronze_gps already dead-letters them (ADR 0010).

Every setting comes from env (ADR 0005), like the ${VARS} of the SQL jobs.
"""

from __future__ import annotations

import io
import json
import os
from datetime import UTC, datetime

from pyflink.common import Row, RowKind, Types
from pyflink.datastream import KeyedProcessFunction, RuntimeContext, StreamExecutionEnvironment
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import DataTypes, Schema, StreamTableEnvironment
from pyflink.table.changelog_mode import ChangelogMode

ENV = os.environ
GPS_TOPIC = ENV.get("GPS_TOPIC", "gps.pings.v1")
ALERTS_TOPIC = ENV.get("ALERTS_TOPIC", "alerts.v1")
BOOTSTRAP = ENV.get("KAFKA_BOOTSTRAP", "redpanda:9092")
REGISTRY = ENV.get("SCHEMA_REGISTRY_URL", "http://redpanda:8081")
JDBC = {
    "url": ENV.get("SERVING_JDBC_URL", "jdbc:postgresql://postgres:5432/control_tower"),
    "username": ENV.get("SERVING_USER", "ct"),
    "password": ENV.get("SERVING_PASSWORD", ""),
}

# One flat event row (pings and order changes) and one flat output row (three kinds).
EVENT_FIELDS = [
    ("kind", Types.STRING()), ("van_id", Types.STRING()), ("event_ts", Types.LONG()),
    ("lat", Types.DOUBLE()), ("lon", Types.DOUBLE()), ("speed_kmh", Types.DOUBLE()),
    ("depot_lat", Types.DOUBLE()), ("depot_lon", Types.DOUBLE()), ("order_id", Types.STRING()),
    ("status", Types.STRING()), ("stop_seq", Types.INT()), ("dest_lat", Types.DOUBLE()),
    ("dest_lon", Types.DOUBLE()), ("promised_end", Types.LONG()), ("lsn", Types.LONG()),
    ("removed", Types.BOOLEAN()),
]  # fmt: skip
OUT_FIELDS = [
    ("kind", Types.STRING()), ("id", Types.STRING()), ("van_id", Types.STRING()),
    ("order_id", Types.STRING()), ("event_ts", Types.LONG()), ("lat", Types.DOUBLE()),
    ("lon", Types.DOUBLE()), ("speed_kmh", Types.DOUBLE()), ("status", Types.STRING()),
    ("promised_end", Types.LONG()), ("eta", Types.LONG()),
    ("distance_km", Types.DOUBLE()), ("alert_type", Types.STRING()),
    ("raised_at", Types.LONG()), ("resolved_at", Types.LONG()),
    ("detail", Types.STRING()), ("alert_event", Types.STRING()), ("deleted", Types.BOOLEAN()),
    ("mark", Types.STRING()),
]  # fmt: skip
OUT_TYPE = Types.ROW_NAMED([n for n, _ in OUT_FIELDS], [t for _, t in OUT_FIELDS])


# Timestamps cross the DataStream part as epoch milliseconds: PyFlink 1.20 has no type that
# converts to the Table side's LocalDateTime; the sinks turn them back with TO_TIMESTAMP_LTZ.
def _ms(ts: datetime | None) -> int | None:
    return None if ts is None else int(ts.timestamp() * 1000)


def _dt(ms: int | None) -> datetime | None:
    return None if ms is None else datetime.fromtimestamp(ms / 1000, UTC)


def _aware(ts: datetime | None) -> datetime | None:
    return None if ts is None else ts.replace(tzinfo=UTC)


class Tower(KeyedProcessFunction):
    """One VanState per van (key), pickled in RocksDB; all rules live in flink.udf.tower."""

    def open(self, ctx: RuntimeContext) -> None:
        self.state = ctx.get_state(ValueStateDescriptor("van", Types.PICKLED_BYTE_ARRAY()))

    def process_element(self, row, ctx):
        from flink.udf import tower

        state = self.state.value() or tower.VanState()
        if row.kind == "ping":
            depot = None if row.depot_lat is None else (row.depot_lat, row.depot_lon)
            event = tower.Ping(row.van_id, _dt(row.event_ts), row.lat, row.lon,
                               row.speed_kmh, depot)  # fmt: skip
        else:
            event = tower.OrderChange(
                row.van_id, row.order_id, row.status, row.stop_seq, row.dest_lat, row.dest_lon,
                _dt(row.promised_end), _dt(row.event_ts), row.lsn, bool(row.removed),
            )  # fmt: skip
        now = datetime.fromtimestamp(ctx.timer_service().current_processing_time() / 1000, UTC)
        outputs = tower.step(state, event, now)
        self.state.update(state)
        for out in outputs:
            yield _to_row(out)


def _to_row(out) -> Row:
    from flink.udf import tower

    none = dict.fromkeys(n for n, _ in OUT_FIELDS)
    if isinstance(out, tower.VehicleLatest):
        values = {**none, "kind": "vehicle", "id": out.van_id, "van_id": out.van_id,
                  "event_ts": _ms(out.event_ts), "lat": out.lat, "lon": out.lon,
                  "speed_kmh": out.speed_kmh, "deleted": False}  # fmt: skip
    elif isinstance(out, tower.OrderEta):
        values = {**none, "kind": "eta", "id": out.order_id, "van_id": out.van_id,
                  "order_id": out.order_id, "event_ts": _ms(out.event_ts),
                  "status": out.status, "promised_end": _ms(out.promised_end),
                  "eta": _ms(out.eta), "distance_km": out.distance_km,
                  "deleted": out.deleted}  # fmt: skip
    elif isinstance(out, tower.EtaMark):
        values = {**none, "kind": "mark", "id": out.order_id, "van_id": out.van_id,
                  "order_id": out.order_id, "event_ts": _ms(out.event_ts), "eta": _ms(out.eta),
                  "promised_end": _ms(out.promised_end), "mark": out.mark,
                  "deleted": False}  # fmt: skip
    else:
        values = {**none, "kind": "alert", "id": out.alert_id, "van_id": out.van_id,
                  "order_id": out.order_id, "event_ts": _ms(out.resolved_at or out.raised_at),
                  "lat": out.lat, "lon": out.lon, "alert_type": out.type,
                  "raised_at": _ms(out.raised_at), "resolved_at": _ms(out.resolved_at),
                  "detail": out.detail, "alert_event": out.event, "deleted": False}  # fmt: skip
    return Row(**{n: values[n] for n, _ in OUT_FIELDS})


class AlertEncoder:
    """alerts.v1 value: registry-framed Avro (ADR 0008), schema id looked up once per worker."""

    def __init__(self) -> None:
        self._writer = None
        self._id = None

    def __call__(self, row) -> Row:
        from fastavro import parse_schema, schemaless_writer

        from contracts.registry import Registry
        from contracts.wire import frame

        if self._writer is None:
            from contracts.decode import SCHEMAS_DIR  # shipped next to contracts/ (python.files)

            schema = json.loads((SCHEMAS_DIR / "alerts.v1-value" / "v1.avsc").read_text())
            self._id = Registry(REGISTRY).id_for("alerts.v1-value", schema)
            self._writer = parse_schema(schema)
        record = {
            "alert_id": row.id, "event": row.alert_event, "type": row.alert_type,
            "van_id": row.van_id, "order_id": row.order_id, "raised_at": _dt(row.raised_at),
            "resolved_at": _dt(row.resolved_at), "lat": row.lat or 0.0, "lon": row.lon or 0.0,
            "detail": row.detail or "",
        }  # fmt: skip
        buf = io.BytesIO()
        schemaless_writer(buf, self._writer, record)
        return Row(row.id.encode(), frame(self._id, buf.getvalue()))


def _eta_changelog(row) -> Row:
    out = Row(row.id, row.van_id, row.status, row.promised_end, row.eta, row.distance_km,
              row.event_ts)  # fmt: skip
    out.set_row_kind(RowKind.DELETE if row.deleted else RowKind.UPDATE_AFTER)
    return out


def _jdbc(table: str) -> str:
    opts = {**JDBC, "connector": "jdbc", "table-name": table,
            "sink.buffer-flush.interval": "200ms", "sink.buffer-flush.max-rows": "500"}  # fmt: skip
    return ", ".join(f"'{k}' = '{v}'" for k, v in opts.items())


EVENT_TYPE = Types.ROW_NAMED([n for n, _ in EVENT_FIELDS], [t for _, t in EVENT_FIELDS])


class _Decoder:
    """Decodes raw Kafka values with the contract code (ADR 0010) inside the DataStream part.
    Python UDFs inside the SQL part hit a planner bug once their ROW result's nested fields
    feed a union and a lookup join (ClassCastException RexFieldAccess → RexCall)."""

    _registry = None

    def registry(self):
        from contracts.registry import Registry

        if _Decoder._registry is None:
            _Decoder._registry = Registry(REGISTRY)
        return _Decoder._registry


class DecodePings(_Decoder):
    def __call__(self, row):
        from contracts.decode import decode_gps

        ping, err = decode_gps(bytes(row.value), _aware(row.ingest_ts), self.registry())
        if err:  # bronze_gps dead-letters it; the alert path skips it
            return
        yield Row(
            "ping", ping["vehicle_id"], _ms(ping["event_ts"]), ping["lat"], ping["lon"],
            ping["speed_kmh"], row.depot_lat, row.depot_lon, None, None, None, None, None, None,
            None, False,
        )  # fmt: skip


class DecodeOrders(_Decoder):
    def __call__(self, row):
        from contracts.cdc import decode_cdc

        change, err = decode_cdc(bytes(row.value), "orders", self.registry())
        if err or change["after"] is None:
            return
        after, before = change["after"], change["before"] or {}

        def event(van_id: str, removed: bool) -> Row:
            return Row(
                "order", van_id, _ms(after["updated_at"]), None, None, None, None, None,
                after["order_id"], after["status"], after["stop_seq"], after["dest_lat"],
                after["dest_lon"], _ms(after["promised_end"]), change["lsn"], removed,
            )  # fmt: skip

        if after["van_id"] is not None:
            yield event(after["van_id"], False)
        old = before.get("van_id")
        if old is not None and old != after["van_id"]:  # a reassigned order leaves its old van
            yield event(old, True)


def build(env: StreamExecutionEnvironment, t_env: StreamTableEnvironment) -> None:
    # Same Iceberg catalog as the SQL jobs (flink/jobs/_init_catalog.sql), from env.
    t_env.execute_sql(f"""
        CREATE CATALOG lakehouse WITH (
          'type' = 'iceberg', 'catalog-type' = 'rest', 'uri' = '{ENV["ICEBERG_CATALOG_URI"]}',
          'warehouse' = '{ENV["WAREHOUSE"]}', 'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
          's3.endpoint' = '{ENV["S3_ENDPOINT"]}', 's3.path-style-access' = 'true',
          's3.access-key-id' = '{ENV["S3_ACCESS_KEY"]}',
          's3.secret-access-key' = '{ENV["S3_SECRET_KEY"]}',
          'client.region' = '{ENV["AWS_REGION"]}')""")
    kafka = f"'connector' = 'kafka', 'properties.bootstrap.servers' = '{BOOTSTRAP}'"
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE gps_raw (
          k BYTES,
          `value` BYTES,
          ingest_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
          proc AS PROCTIME()
        ) WITH ({kafka}, 'topic' = '{GPS_TOPIC}',
          'key.format' = 'raw', 'key.fields' = 'k',
          'value.format' = 'raw', 'value.fields-include' = 'EXCEPT_KEY',
          'properties.group.id' = 'flink-control-tower-gps',
          'scan.startup.mode' = 'latest-offset',
          'scan.topic-partition-discovery.interval' = '1 min')""")
    # Startup modes apply only without state (a restore uses the offsets in the checkpoint).
    # A new job needs no GPS history (positions refresh every 5 s), but every order change: the
    # open orders are the whole changelog (Debezium snapshot + changes).
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE orders_raw (
          `value` BYTES
        ) WITH ({kafka}, 'topic' = 'cdc.public.orders', 'format' = 'raw',
          'properties.group.id' = 'flink-control-tower-orders',
          'scan.startup.mode' = 'earliest-offset')""")
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE van_depots (
          van_id STRING, depot_lat DOUBLE, depot_lon DOUBLE, PRIMARY KEY (van_id) NOT ENFORCED
        ) WITH ({_jdbc("serving.van_depots")}, 'lookup.cache' = 'PARTIAL',
          'lookup.partial-cache.expire-after-write' = '1 h',
          'lookup.partial-cache.max-rows' = '1000')""")
    # The Kafka key is the vehicle id, so the depot lookup needs no decoding.
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW gps_keyed AS
        SELECT CAST(k AS STRING) AS van_id, `value`, ingest_ts, proc FROM gps_raw""")
    gps = t_env.sql_query("""
        SELECT g.`value`, CAST(g.ingest_ts AS TIMESTAMP(3)) AS ingest_ts, v.depot_lat, v.depot_lon
        FROM gps_keyed AS g
        LEFT JOIN van_depots FOR SYSTEM_TIME AS OF g.proc AS v ON g.van_id = v.van_id""")
    pings = t_env.to_data_stream(gps).flat_map(DecodePings(), output_type=EVENT_TYPE)
    orders = t_env.to_data_stream(t_env.from_path("orders_raw")).flat_map(
        DecodeOrders(), output_type=EVENT_TYPE
    )
    events = pings.union(orders)

    outputs = (
        events.key_by(lambda r: r.van_id, key_type=Types.STRING())
        .process(Tower(), output_type=OUT_TYPE)
        .name("tower")
        .uid("tower")  # a fixed id, so the per-van state restores after the job graph changes
    )
    vehicles = outputs.filter(lambda r: r.kind == "vehicle").map(
        lambda r: Row(r.van_id, r.event_ts, r.lat, r.lon, r.speed_kmh),
        output_type=Types.ROW_NAMED(
            ["van_id", "event_ts", "lat", "lon", "speed_kmh"],
            [
                Types.STRING(),
                Types.LONG(),
                Types.DOUBLE(),
                Types.DOUBLE(),
                Types.DOUBLE(),
            ],
        ),
    )
    etas = outputs.filter(lambda r: r.kind == "eta").map(
        _eta_changelog,
        output_type=Types.ROW_NAMED(
            ["order_id", "van_id", "status", "promised_end", "eta", "distance_km", "event_ts"],
            [
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.LONG(),
                Types.LONG(),
                Types.DOUBLE(),
                Types.LONG(),
            ],
        ),  # fmt: skip
    )
    alerts = outputs.filter(lambda r: r.kind == "alert")
    alert_rows = alerts.map(
        lambda r: Row(r.id, r.alert_type, r.van_id, r.order_id, r.raised_at, r.resolved_at,
                      r.lat, r.lon, r.detail),
        output_type=Types.ROW_NAMED(
            ["alert_id", "type", "van_id", "order_id", "raised_at", "resolved_at", "lat", "lon",
             "detail"],
            [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
             Types.LONG(), Types.LONG(), Types.DOUBLE(), Types.DOUBLE(),
             Types.STRING()],
        ),
    )  # fmt: skip
    alert_bytes = alerts.map(
        AlertEncoder(),
        output_type=Types.ROW_NAMED(["k", "v"], [Types.PRIMITIVE_ARRAY(Types.BYTE()),
                                                 Types.PRIMITIVE_ARRAY(Types.BYTE())]),
    )  # fmt: skip

    t_env.create_temporary_view("vehicles", t_env.from_data_stream(vehicles))
    t_env.create_temporary_view(
        "etas",
        t_env.from_changelog_stream(
            etas,
            Schema.new_builder()
            .column("order_id", DataTypes.STRING().not_null())
            .column("van_id", DataTypes.STRING())
            .column("status", DataTypes.STRING())
            .column("promised_end", DataTypes.BIGINT())
            .column("eta", DataTypes.BIGINT())
            .column("distance_km", DataTypes.DOUBLE())
            .column("event_ts", DataTypes.BIGINT())
            .primary_key("order_id")
            .build(),
            ChangelogMode.upsert(),
        ),
    )
    t_env.create_temporary_view("alert_rows", t_env.from_data_stream(alert_rows))
    t_env.create_temporary_view("alert_bytes", t_env.from_data_stream(alert_bytes))
    # History for gold (spec 007): every alert event and the ETA marks, appended to bronze.
    t_env.create_temporary_view(
        "alert_history",
        t_env.from_data_stream(
            alerts.map(
                lambda r: Row(r.id, r.alert_event, r.alert_type, r.van_id, r.order_id,
                              r.raised_at, r.resolved_at, r.lat, r.lon, r.detail, r.event_ts),
                output_type=Types.ROW_NAMED(
                    ["alert_id", "event", "type", "van_id", "order_id", "raised_at",
                     "resolved_at", "lat", "lon", "detail", "event_ts"],
                    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
                     Types.STRING(), Types.LONG(), Types.LONG(), Types.DOUBLE(), Types.DOUBLE(),
                     Types.STRING(), Types.LONG()],
                ),
            )
        ),
    )  # fmt: skip
    t_env.create_temporary_view(
        "eta_marks",
        t_env.from_data_stream(
            outputs.filter(lambda r: r.kind == "mark").map(
                lambda r: Row(r.order_id, r.van_id, r.mark, r.eta, r.promised_end, r.event_ts),
                output_type=Types.ROW_NAMED(
                    ["order_id", "van_id", "mark", "eta", "promised_end", "event_ts"],
                    [Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG(),
                     Types.LONG(), Types.LONG()],
                ),
            )
        ),
    )  # fmt: skip

    for name, cols in {
        "vehicle_latest": "van_id STRING, event_ts TIMESTAMP(3), lat DOUBLE, lon DOUBLE, "
        "speed_kmh DOUBLE, PRIMARY KEY (van_id) NOT ENFORCED",
        "order_eta": "order_id STRING, van_id STRING, status STRING, promised_end TIMESTAMP(3), "
        "eta TIMESTAMP(3), distance_km DOUBLE, event_ts TIMESTAMP(3), "
        "PRIMARY KEY (order_id) NOT ENFORCED",
        "alerts": "alert_id STRING, type STRING, van_id STRING, order_id STRING, "
        "raised_at TIMESTAMP(3), resolved_at TIMESTAMP(3), lat DOUBLE, lon DOUBLE, "
        "detail STRING, PRIMARY KEY (alert_id) NOT ENFORCED",
    }.items():
        t_env.execute_sql(
            f"CREATE TEMPORARY TABLE sink_{name} ({cols}) WITH ({_jdbc('serving.' + name)})"
        )
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE sink_alerts_topic (k BYTES, v BYTES) WITH (
          'connector' = 'kafka', 'topic' = '{ALERTS_TOPIC}',
          'properties.bootstrap.servers' = '{BOOTSTRAP}',
          'key.format' = 'raw', 'key.fields' = 'k', 'value.format' = 'raw',
          'value.fields-include' = 'EXCEPT_KEY',
          'properties.compression.type' = 'zstd',
          'sink.delivery-guarantee' = 'exactly-once',
          'sink.transactional-id-prefix' = 'control-tower-alerts',
          'properties.transaction.timeout.ms' = '900000')""")

    def ts(col: str) -> str:  # epoch ms → TIMESTAMP(3) in UTC (the session time zone)
        return f"CAST(TO_TIMESTAMP_LTZ({col}, 3) AS TIMESTAMP(3))"

    sinks = t_env.create_statement_set()
    sinks.add_insert_sql(
        f"INSERT INTO sink_vehicle_latest SELECT van_id, {ts('event_ts')}, lat, lon, speed_kmh "
        "FROM vehicles"
    )
    sinks.add_insert_sql(
        f"INSERT INTO sink_order_eta SELECT order_id, van_id, status, {ts('promised_end')}, "
        f"{ts('eta')}, distance_km, {ts('event_ts')} FROM etas"
    )
    sinks.add_insert_sql(
        f"INSERT INTO sink_alerts SELECT alert_id, type, van_id, order_id, {ts('raised_at')}, "
        f"{ts('resolved_at')}, lat, lon, detail FROM alert_rows"
    )
    sinks.add_insert_sql("INSERT INTO sink_alerts_topic SELECT k, v FROM alert_bytes")

    def ltz(col: str) -> str:  # epoch ms → TIMESTAMP_LTZ(6), the bronze column type
        return f"CAST(TO_TIMESTAMP_LTZ({col}, 3) AS TIMESTAMP_LTZ(6))"

    sinks.add_insert_sql(
        "INSERT INTO lakehouse.bronze.alert_events SELECT alert_id, event, type, van_id, "
        f"order_id, {ltz('raised_at')}, {ltz('resolved_at')}, lat, lon, detail, "
        f"{ltz('event_ts')} FROM alert_history"
    )
    sinks.add_insert_sql(
        f"INSERT INTO lakehouse.bronze.eta_marks SELECT order_id, van_id, mark, {ltz('eta')}, "
        f"{ltz('promised_end')}, {ltz('event_ts')} FROM eta_marks"
    )
    sinks.attach_as_datastream()


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # about 200 vans of state; one subtask is plenty (ADR 0013)
    env.enable_checkpointing(int(ENV.get("CHECKPOINT_INTERVAL_MS", "30000")))  # like the SQL jobs
    t_env = StreamTableEnvironment.create(env)
    cfg = t_env.get_config()
    for key, value in {
        "pipeline.name": "control_tower",
        "table.local-time-zone": "UTC",
        # latency (NFR-1): hand Python small bundles quickly instead of waiting for 1 s / 100k rows
        "python.fn-execution.bundle.time": "100",
        "python.fn-execution.bundle.size": "1000",
        "python.fn-execution.arrow.batch.size": "1000",
    }.items():
        cfg.set(key, value)
    build(env, t_env)
    env.execute("control_tower")


if __name__ == "__main__":
    main()

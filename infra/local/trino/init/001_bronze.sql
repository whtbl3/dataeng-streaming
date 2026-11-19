-- Bronze layer DDL. Created in Trino (not Flink) because Flink DDL cannot declare
-- Iceberg hidden partition transforms such as day(event_ts). Idempotent.
CREATE SCHEMA IF NOT EXISTS lakehouse.bronze;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.gps_pings (
    vehicle_id      VARCHAR,
    event_ts        TIMESTAMP(6) WITH TIME ZONE,
    lat             DOUBLE,
    lon             DOUBLE,
    speed_kmh       DOUBLE,
    heading_deg     DOUBLE,
    ingest_ts       TIMESTAMP(6) WITH TIME ZONE,
    kafka_partition INTEGER,
    kafka_offset    BIGINT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(event_ts)'],
    sorted_by = ARRAY['vehicle_id', 'event_ts']  -- applied when compacted (spec 005)
);

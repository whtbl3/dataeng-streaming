-- Silver layer DDL (spec 004). Created in Trino because Flink DDL cannot declare partition
-- transforms. Flink writes upserts keyed by each table's Iceberg identifier fields, which Trino
-- cannot set: flink-submit sets them with infra/local/iceberg/table_admin.py (ADR 0011).
-- Key columns are NOT NULL because Iceberg identifier fields must be required. Idempotent.
CREATE SCHEMA IF NOT EXISTS lakehouse.silver;

-- One row per real ping. Key (vehicle_id, event_ts); the first copy to arrive wins.
CREATE TABLE IF NOT EXISTS lakehouse.silver.gps_pings (
    vehicle_id      VARCHAR NOT NULL,
    event_ts        TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    lat             DOUBLE,
    lon             DOUBLE,
    speed_kmh       DOUBLE,
    heading_deg     DOUBLE,
    ingest_ts       TIMESTAMP(6) WITH TIME ZONE,  -- when the device sent the first copy
    lateness_s      BIGINT,                       -- ingest_ts - event_ts, in seconds
    is_late         BOOLEAN                       -- lateness_s > 30 (the watermark allowance)
)
WITH (
    format = 'PARQUET',
    format_version = 2,
    -- One partition a day (about 80 MB at 200 vans), files sorted by van then time when compacted,
    -- so a one-van query skips most files by their min/max stats (spec 005, ADR 0012).
    partitioning = ARRAY['day(event_ts)'],
    sorted_by = ARRAY['vehicle_id', 'event_ts']
);

-- One row per order: the version with the highest commit position (source_lsn).
-- created_at is in the key because Iceberg upserts need the partition source column; it never
-- changes for an order, so the key is still one row per order.
CREATE TABLE IF NOT EXISTS lakehouse.silver.orders (
    order_id            VARCHAR NOT NULL,
    created_at          TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    depot_id            VARCHAR,
    van_id              VARCHAR,
    stop_seq            INTEGER,
    dest_lat            DOUBLE,
    dest_lon            DOUBLE,
    promised_start      TIMESTAMP(6) WITH TIME ZONE,
    promised_end        TIMESTAMP(6) WITH TIME ZONE,
    status              VARCHAR,
    failure_reason      VARCHAR,
    assigned_at         TIMESTAMP(6) WITH TIME ZONE,
    out_for_delivery_at TIMESTAMP(6) WITH TIME ZONE,
    delivered_at        TIMESTAMP(6) WITH TIME ZONE,
    failed_at           TIMESTAMP(6) WITH TIME ZONE,
    updated_at          TIMESTAMP(6) WITH TIME ZONE,
    last_lsn            BIGINT,
    last_commit_ts      TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    format_version = 2,
    partitioning = ARRAY['month(created_at)']
);

-- One row per status an order reached, with the commit that first recorded it.
CREATE TABLE IF NOT EXISTS lakehouse.silver.order_status_history (
    order_id        VARCHAR NOT NULL,
    status          VARCHAR NOT NULL,
    status_ts       TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    source_lsn      BIGINT,
    commit_ts       TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET',
    format_version = 2,
    partitioning = ARRAY['day(status_ts)']
);

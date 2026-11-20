-- Empty source tables for `make dbt-ci` (memory catalog): the same columns and types as
-- infra/lakehouse/tables.py, so dbt can build the models empty and run its unit tests.
CREATE SCHEMA IF NOT EXISTS lakehouse.silver;
CREATE SCHEMA IF NOT EXISTS lakehouse.bronze;
CREATE SCHEMA IF NOT EXISTS lakehouse.gold;
CREATE SCHEMA IF NOT EXISTS lakehouse.gold_staging;
CREATE TABLE lakehouse.silver.orders (
    order_id VARCHAR, created_at TIMESTAMP(6) WITH TIME ZONE, depot_id VARCHAR, van_id VARCHAR,
    stop_seq INTEGER, dest_lat DOUBLE, dest_lon DOUBLE,
    promised_start TIMESTAMP(6) WITH TIME ZONE, promised_end TIMESTAMP(6) WITH TIME ZONE,
    status VARCHAR, failure_reason VARCHAR, assigned_at TIMESTAMP(6) WITH TIME ZONE,
    out_for_delivery_at TIMESTAMP(6) WITH TIME ZONE, delivered_at TIMESTAMP(6) WITH TIME ZONE,
    failed_at TIMESTAMP(6) WITH TIME ZONE, updated_at TIMESTAMP(6) WITH TIME ZONE,
    last_lsn BIGINT, last_commit_ts TIMESTAMP(6) WITH TIME ZONE
);
CREATE TABLE lakehouse.silver.order_status_history (
    order_id VARCHAR, status VARCHAR, status_ts TIMESTAMP(6) WITH TIME ZONE, source_lsn BIGINT,
    commit_ts TIMESTAMP(6) WITH TIME ZONE
);
CREATE TABLE lakehouse.silver.gps_pings (
    vehicle_id VARCHAR, event_ts TIMESTAMP(6) WITH TIME ZONE, lat DOUBLE, lon DOUBLE,
    speed_kmh DOUBLE, heading_deg DOUBLE, ingest_ts TIMESTAMP(6) WITH TIME ZONE,
    lateness_s BIGINT, is_late BOOLEAN
);
CREATE TABLE lakehouse.bronze.vans_cdc (
    op VARCHAR,
    "before" ROW(van_id VARCHAR, depot_id VARCHAR, plate VARCHAR, capacity INTEGER,
                 created_at TIMESTAMP(6) WITH TIME ZONE, updated_at TIMESTAMP(6) WITH TIME ZONE),
    "after" ROW(van_id VARCHAR, depot_id VARCHAR, plate VARCHAR, capacity INTEGER,
                created_at TIMESTAMP(6) WITH TIME ZONE, updated_at TIMESTAMP(6) WITH TIME ZONE),
    snapshot VARCHAR, source_lsn BIGINT, source_txid BIGINT,
    commit_ts TIMESTAMP(6) WITH TIME ZONE, capture_ts TIMESTAMP(6) WITH TIME ZONE,
    kafka_partition INTEGER, kafka_offset BIGINT, ingest_ts TIMESTAMP(6) WITH TIME ZONE
);
CREATE TABLE lakehouse.bronze.depots_cdc (
    op VARCHAR,
    "before" ROW(depot_id VARCHAR, name VARCHAR, lat DOUBLE, lon DOUBLE,
                 created_at TIMESTAMP(6) WITH TIME ZONE, updated_at TIMESTAMP(6) WITH TIME ZONE),
    "after" ROW(depot_id VARCHAR, name VARCHAR, lat DOUBLE, lon DOUBLE,
                created_at TIMESTAMP(6) WITH TIME ZONE, updated_at TIMESTAMP(6) WITH TIME ZONE),
    snapshot VARCHAR, source_lsn BIGINT, source_txid BIGINT,
    commit_ts TIMESTAMP(6) WITH TIME ZONE, capture_ts TIMESTAMP(6) WITH TIME ZONE,
    kafka_partition INTEGER, kafka_offset BIGINT, ingest_ts TIMESTAMP(6) WITH TIME ZONE
);
CREATE TABLE lakehouse.bronze.alert_events (
    alert_id VARCHAR, event VARCHAR, type VARCHAR, van_id VARCHAR, order_id VARCHAR,
    raised_at TIMESTAMP(6) WITH TIME ZONE, resolved_at TIMESTAMP(6) WITH TIME ZONE,
    lat DOUBLE, lon DOUBLE, detail VARCHAR, event_ts TIMESTAMP(6) WITH TIME ZONE
);
CREATE TABLE lakehouse.bronze.eta_marks (
    order_id VARCHAR, van_id VARCHAR, mark VARCHAR, eta TIMESTAMP(6) WITH TIME ZONE,
    promised_end TIMESTAMP(6) WITH TIME ZONE, event_ts TIMESTAMP(6) WITH TIME ZONE
);

-- Bronze CDC changelogs (spec 002): one append-only table per source table, raw Debezium
-- envelope with typed before/after state. No merging here; current state is built in silver.
CREATE TABLE IF NOT EXISTS lakehouse.bronze.depots_cdc (
    op              VARCHAR,
    "before"        ROW(depot_id VARCHAR, name VARCHAR, lat DOUBLE, lon DOUBLE,
                        created_at TIMESTAMP(6) WITH TIME ZONE,
                        updated_at TIMESTAMP(6) WITH TIME ZONE),
    "after"         ROW(depot_id VARCHAR, name VARCHAR, lat DOUBLE, lon DOUBLE,
                        created_at TIMESTAMP(6) WITH TIME ZONE,
                        updated_at TIMESTAMP(6) WITH TIME ZONE),
    snapshot        VARCHAR,
    source_lsn      BIGINT,
    source_txid     BIGINT,
    commit_ts       TIMESTAMP(6) WITH TIME ZONE,
    capture_ts      TIMESTAMP(6) WITH TIME ZONE,
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    ingest_ts       TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS lakehouse.bronze.vans_cdc (
    op              VARCHAR,
    "before"        ROW(van_id VARCHAR, depot_id VARCHAR, plate VARCHAR, capacity INTEGER,
                        created_at TIMESTAMP(6) WITH TIME ZONE,
                        updated_at TIMESTAMP(6) WITH TIME ZONE),
    "after"         ROW(van_id VARCHAR, depot_id VARCHAR, plate VARCHAR, capacity INTEGER,
                        created_at TIMESTAMP(6) WITH TIME ZONE,
                        updated_at TIMESTAMP(6) WITH TIME ZONE),
    snapshot        VARCHAR,
    source_lsn      BIGINT,
    source_txid     BIGINT,
    commit_ts       TIMESTAMP(6) WITH TIME ZONE,
    capture_ts      TIMESTAMP(6) WITH TIME ZONE,
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    ingest_ts       TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS lakehouse.bronze.orders_cdc (
    op              VARCHAR,
    "before"        ROW(order_id VARCHAR, depot_id VARCHAR, van_id VARCHAR, stop_seq INTEGER,
                        dest_lat DOUBLE, dest_lon DOUBLE,
                        promised_start TIMESTAMP(6) WITH TIME ZONE,
                        promised_end TIMESTAMP(6) WITH TIME ZONE,
                        status VARCHAR, failure_reason VARCHAR,
                        created_at TIMESTAMP(6) WITH TIME ZONE,
                        assigned_at TIMESTAMP(6) WITH TIME ZONE,
                        out_for_delivery_at TIMESTAMP(6) WITH TIME ZONE,
                        delivered_at TIMESTAMP(6) WITH TIME ZONE,
                        failed_at TIMESTAMP(6) WITH TIME ZONE,
                        updated_at TIMESTAMP(6) WITH TIME ZONE),
    "after"         ROW(order_id VARCHAR, depot_id VARCHAR, van_id VARCHAR, stop_seq INTEGER,
                        dest_lat DOUBLE, dest_lon DOUBLE,
                        promised_start TIMESTAMP(6) WITH TIME ZONE,
                        promised_end TIMESTAMP(6) WITH TIME ZONE,
                        status VARCHAR, failure_reason VARCHAR,
                        created_at TIMESTAMP(6) WITH TIME ZONE,
                        assigned_at TIMESTAMP(6) WITH TIME ZONE,
                        out_for_delivery_at TIMESTAMP(6) WITH TIME ZONE,
                        delivered_at TIMESTAMP(6) WITH TIME ZONE,
                        failed_at TIMESTAMP(6) WITH TIME ZONE,
                        updated_at TIMESTAMP(6) WITH TIME ZONE),
    snapshot        VARCHAR,
    source_lsn      BIGINT,
    source_txid     BIGINT,
    commit_ts       TIMESTAMP(6) WITH TIME ZONE,
    capture_ts      TIMESTAMP(6) WITH TIME ZONE,
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    ingest_ts       TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET', partitioning = ARRAY['day(commit_ts)']);

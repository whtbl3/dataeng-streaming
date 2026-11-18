-- Dead-letter table (spec 003): every rejected message from every bronze job, with the reason
-- and the raw bytes, so analysts can find and explain rejects with SQL. Idempotent.
CREATE TABLE IF NOT EXISTS lakehouse.bronze.dlq_events (
    source_topic    VARCHAR,
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    ingest_ts       TIMESTAMP(6) WITH TIME ZONE,
    reason          VARCHAR,
    detail          VARCHAR,
    schema_id       INTEGER,
    raw_value       VARBINARY
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(ingest_ts)']
);

-- silver_gps: bronze.gps_pings → silver.gps_pings, one row per real ping (spec 004, ADR 0011).
-- Reads bronze incrementally (bronze is the only source of truth, so a rebuild is this same job
-- started on an empty silver table). Two layers keep silver unique:
--   1. Top-1 per (vehicle_id, event_ts) ordered by arrival (ingest_ts, then partition/offset):
--      the first copy wins and later copies emit nothing while the key is in state (TTL 24 h).
--   2. Upsert on the table's identifier fields: a copy that comes after the TTL replaces the same
--      row instead of adding one.
-- Lateness comes from the data (ingest_ts - event_ts), so every rebuild flags the same rows.
SET 'pipeline.name' = 'silver_gps';
-- GPS jobs can read with as many parallel readers as the topic has partitions (6, spec 005).
SET 'parallelism.default' = '${GPS_PARALLELISM}';
SET 'table.exec.state.ttl' = '24 h';

-- A connector table (not the catalog table) so the source can declare a watermark. The watermark
-- needs TIMESTAMP_LTZ(3) or less, so it sits on a computed copy of event_ts. Nothing in silver
-- uses event-time operators yet; feature 6 reads this watermark (ADR 0006: 30 s allowance).
CREATE TEMPORARY TABLE bronze_gps (
  vehicle_id      STRING,
  event_ts        TIMESTAMP_LTZ(6),
  lat             DOUBLE,
  lon             DOUBLE,
  speed_kmh       DOUBLE,
  heading_deg     DOUBLE,
  ingest_ts       TIMESTAMP_LTZ(6),
  kafka_partition INT,
  kafka_offset    BIGINT,
  event_time AS CAST(event_ts AS TIMESTAMP_LTZ(3)),
  WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'bronze_src',
  ${ICEBERG_CATALOG_OPTIONS},
  'catalog-database' = 'bronze',
  'catalog-table' = 'gps_pings',
  'streaming' = 'true',
  'monitor-interval' = '10s',
  'starting-strategy' = 'TABLE_SCAN_THEN_INCREMENTAL',
  -- Bronze is append-only; its only other snapshots come from maintenance (spec 005): compaction
  -- ("replace", always skipped) and retention deletes, which must not reach silver.
  'streaming-skip-delete-snapshots' = 'true',
  'streaming-skip-overwrite-snapshots' = 'true'
);

INSERT INTO lakehouse.silver.gps_pings /*+ OPTIONS('upsert-enabled' = 'true') */
SELECT vehicle_id, event_ts, lat, lon, speed_kmh, heading_deg, ingest_ts,
       CAST(TIMESTAMPDIFF(SECOND, event_ts, ingest_ts) AS BIGINT) AS lateness_s,
       TIMESTAMPDIFF(SECOND, event_ts, ingest_ts) > 30 AS is_late
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY vehicle_id, event_ts
                            ORDER BY ingest_ts, kafka_partition, kafka_offset) AS rn
  FROM bronze_gps
)
WHERE rn = 1;

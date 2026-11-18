-- bronze_gps: raw GPS pings from Kafka → Iceberg bronze (append-only, exactly-once).
-- Catalog and checkpointing come from _init_catalog.sql.
SET 'pipeline.name' = 'bronze_gps';

CREATE TEMPORARY TABLE gps_pings_source (
  vehicle_id      STRING,
  event_ts        TIMESTAMP_LTZ(3),
  lat             DOUBLE,
  lon             DOUBLE,
  speed_kmh       DOUBLE,
  heading_deg     DOUBLE,
  ingest_ts       TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset    BIGINT METADATA FROM 'offset' VIRTUAL,
  WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = '${GPS_TOPIC}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id' = 'flink-bronze-gps',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'false'
);

INSERT INTO lakehouse.bronze.gps_pings
SELECT
  vehicle_id,
  CAST(event_ts AS TIMESTAMP_LTZ(6)),
  lat,
  lon,
  speed_kmh,
  heading_deg,
  CAST(ingest_ts AS TIMESTAMP_LTZ(6)),
  kafka_partition,
  kafka_offset
FROM gps_pings_source;

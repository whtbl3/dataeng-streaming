-- bronze_gps: raw GPS pings from Kafka → Iceberg bronze (append-only, exactly-once).
-- ${VARS} are filled from env by submit.sh, so the same SQL runs locally and on AWS.

SET 'pipeline.name' = 'bronze_gps';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '${CHECKPOINT_INTERVAL}';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'table.local-time-zone' = 'UTC';

CREATE CATALOG lakehouse WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = '${ICEBERG_CATALOG_URI}',
  'warehouse' = '${WAREHOUSE}',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = '${S3_ENDPOINT}',
  's3.path-style-access' = 'true',
  's3.access-key-id' = '${S3_ACCESS_KEY}',
  's3.secret-access-key' = '${S3_SECRET_KEY}',
  'client.region' = '${AWS_REGION}'
);

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

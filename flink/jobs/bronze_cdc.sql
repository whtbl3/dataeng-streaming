-- bronze_cdc: raw Debezium change events → Iceberg bronze changelogs (append-only).
-- The envelope is read with the plain `json` format on purpose: bronze keeps every change
-- as a row (no changelog/upsert semantics). Current state is built in silver (feature 4).
SET 'pipeline.name' = 'bronze_cdc';

CREATE TEMPORARY TABLE depots_cdc_source (
  op       STRING,
  `before` ROW<depot_id STRING, name STRING, lat DOUBLE, lon DOUBLE,
               created_at TIMESTAMP_LTZ(6), updated_at TIMESTAMP_LTZ(6)>,
  `after`  ROW<depot_id STRING, name STRING, lat DOUBLE, lon DOUBLE,
               created_at TIMESTAMP_LTZ(6), updated_at TIMESTAMP_LTZ(6)>,
  source   ROW<ts_ms BIGINT, snapshot STRING, txId BIGINT, lsn BIGINT>,
  ts_ms    BIGINT,
  commit_ts AS TO_TIMESTAMP_LTZ(source.ts_ms, 3),
  ingest_ts       TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset    BIGINT METADATA FROM 'offset' VIRTUAL,
  WATERMARK FOR commit_ts AS commit_ts - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.public.depots',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id' = 'flink-bronze-cdc',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'false'
);

CREATE TEMPORARY TABLE vans_cdc_source (
  op       STRING,
  `before` ROW<van_id STRING, depot_id STRING, plate STRING, capacity INT,
               created_at TIMESTAMP_LTZ(6), updated_at TIMESTAMP_LTZ(6)>,
  `after`  ROW<van_id STRING, depot_id STRING, plate STRING, capacity INT,
               created_at TIMESTAMP_LTZ(6), updated_at TIMESTAMP_LTZ(6)>,
  source   ROW<ts_ms BIGINT, snapshot STRING, txId BIGINT, lsn BIGINT>,
  ts_ms    BIGINT,
  commit_ts AS TO_TIMESTAMP_LTZ(source.ts_ms, 3),
  ingest_ts       TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset    BIGINT METADATA FROM 'offset' VIRTUAL,
  WATERMARK FOR commit_ts AS commit_ts - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.public.vans',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id' = 'flink-bronze-cdc',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'false'
);

CREATE TEMPORARY TABLE orders_cdc_source (
  op       STRING,
  `before` ROW<order_id STRING, depot_id STRING, van_id STRING, stop_seq INT,
               dest_lat DOUBLE, dest_lon DOUBLE,
               promised_start TIMESTAMP_LTZ(6), promised_end TIMESTAMP_LTZ(6),
               status STRING, failure_reason STRING,
               created_at TIMESTAMP_LTZ(6), assigned_at TIMESTAMP_LTZ(6),
               out_for_delivery_at TIMESTAMP_LTZ(6), delivered_at TIMESTAMP_LTZ(6),
               failed_at TIMESTAMP_LTZ(6), updated_at TIMESTAMP_LTZ(6)>,
  `after`  ROW<order_id STRING, depot_id STRING, van_id STRING, stop_seq INT,
               dest_lat DOUBLE, dest_lon DOUBLE,
               promised_start TIMESTAMP_LTZ(6), promised_end TIMESTAMP_LTZ(6),
               status STRING, failure_reason STRING,
               created_at TIMESTAMP_LTZ(6), assigned_at TIMESTAMP_LTZ(6),
               out_for_delivery_at TIMESTAMP_LTZ(6), delivered_at TIMESTAMP_LTZ(6),
               failed_at TIMESTAMP_LTZ(6), updated_at TIMESTAMP_LTZ(6)>,
  source   ROW<ts_ms BIGINT, snapshot STRING, txId BIGINT, lsn BIGINT>,
  ts_ms    BIGINT,
  commit_ts AS TO_TIMESTAMP_LTZ(source.ts_ms, 3),
  ingest_ts       TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset    BIGINT METADATA FROM 'offset' VIRTUAL,
  WATERMARK FOR commit_ts AS commit_ts - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.public.orders',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id' = 'flink-bronze-cdc',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'false'
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO lakehouse.bronze.depots_cdc
  SELECT op, `before`, `after`, source.snapshot, source.lsn, source.txId,
         CAST(commit_ts AS TIMESTAMP_LTZ(6)), CAST(TO_TIMESTAMP_LTZ(ts_ms, 3) AS TIMESTAMP_LTZ(6)),
         kafka_partition, kafka_offset, CAST(ingest_ts AS TIMESTAMP_LTZ(6))
  FROM depots_cdc_source;

  INSERT INTO lakehouse.bronze.vans_cdc
  SELECT op, `before`, `after`, source.snapshot, source.lsn, source.txId,
         CAST(commit_ts AS TIMESTAMP_LTZ(6)), CAST(TO_TIMESTAMP_LTZ(ts_ms, 3) AS TIMESTAMP_LTZ(6)),
         kafka_partition, kafka_offset, CAST(ingest_ts AS TIMESTAMP_LTZ(6))
  FROM vans_cdc_source;

  INSERT INTO lakehouse.bronze.orders_cdc
  SELECT op, `before`, `after`, source.snapshot, source.lsn, source.txId,
         CAST(commit_ts AS TIMESTAMP_LTZ(6)), CAST(TO_TIMESTAMP_LTZ(ts_ms, 3) AS TIMESTAMP_LTZ(6)),
         kafka_partition, kafka_offset, CAST(ingest_ts AS TIMESTAMP_LTZ(6))
  FROM orders_cdc_source;
END;

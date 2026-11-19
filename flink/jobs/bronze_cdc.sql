-- bronze_cdc: Debezium Avro change events → Iceberg bronze changelogs (append-only, exactly-once).
-- Bronze keeps every change as a row (no changelog/upsert semantics); current state is built in
-- silver (feature 4). As in bronze_gps (spec 003, ADR 0010), each source reads raw bytes and one
-- Python UDF per table decodes them; rejects go to bronze.dlq_events and dlq.<topic>, so a bad
-- message never stops the job. Sinks read only the UDF's ROW, so each message is decoded once.
SET 'pipeline.name' = 'bronze_cdc';

CREATE TEMPORARY FUNCTION decode_depots_cdc AS 'contracts.udf.decode_depots_cdc_udf' LANGUAGE PYTHON;

CREATE TEMPORARY TABLE depots_raw (
  `value`         BYTES,
  ingest_ts       TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset    BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.public.depots',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id' = 'flink-bronze-cdc',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'scan.topic-partition-discovery.interval' = '1 min',
  'format' = 'raw'
);

CREATE TEMPORARY VIEW depots_decoded AS
SELECT decode_depots_cdc(`value`, ingest_ts, kafka_partition, kafka_offset,
                      '${SCHEMA_REGISTRY_URL}') AS d
FROM depots_raw;

CREATE TEMPORARY TABLE depots_dlq_topic (
  `value`  BYTES,
  headers  MAP<STRING, BYTES> METADATA
) WITH (
  'connector' = 'kafka',
  'topic' = 'dlq.cdc.public.depots',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'format' = 'raw',
  'sink.delivery-guarantee' = 'exactly-once',
  'properties.compression.type' = 'zstd',
  'sink.transactional-id-prefix' = 'bronze-cdc-depots-dlq',
  'properties.transaction.timeout.ms' = '900000'
);

CREATE TEMPORARY FUNCTION decode_vans_cdc AS 'contracts.udf.decode_vans_cdc_udf' LANGUAGE PYTHON;

CREATE TEMPORARY TABLE vans_raw (
  `value`         BYTES,
  ingest_ts       TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset    BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.public.vans',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id' = 'flink-bronze-cdc',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'scan.topic-partition-discovery.interval' = '1 min',
  'format' = 'raw'
);

CREATE TEMPORARY VIEW vans_decoded AS
SELECT decode_vans_cdc(`value`, ingest_ts, kafka_partition, kafka_offset,
                      '${SCHEMA_REGISTRY_URL}') AS d
FROM vans_raw;

CREATE TEMPORARY TABLE vans_dlq_topic (
  `value`  BYTES,
  headers  MAP<STRING, BYTES> METADATA
) WITH (
  'connector' = 'kafka',
  'topic' = 'dlq.cdc.public.vans',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'format' = 'raw',
  'sink.delivery-guarantee' = 'exactly-once',
  'properties.compression.type' = 'zstd',
  'sink.transactional-id-prefix' = 'bronze-cdc-vans-dlq',
  'properties.transaction.timeout.ms' = '900000'
);

CREATE TEMPORARY FUNCTION decode_orders_cdc AS 'contracts.udf.decode_orders_cdc_udf' LANGUAGE PYTHON;

CREATE TEMPORARY TABLE orders_raw (
  `value`         BYTES,
  ingest_ts       TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset    BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.public.orders',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id' = 'flink-bronze-cdc',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'scan.topic-partition-discovery.interval' = '1 min',
  'format' = 'raw'
);

CREATE TEMPORARY VIEW orders_decoded AS
SELECT decode_orders_cdc(`value`, ingest_ts, kafka_partition, kafka_offset,
                      '${SCHEMA_REGISTRY_URL}') AS d
FROM orders_raw;

CREATE TEMPORARY TABLE orders_dlq_topic (
  `value`  BYTES,
  headers  MAP<STRING, BYTES> METADATA
) WITH (
  'connector' = 'kafka',
  'topic' = 'dlq.cdc.public.orders',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'format' = 'raw',
  'sink.delivery-guarantee' = 'exactly-once',
  'properties.compression.type' = 'zstd',
  'sink.transactional-id-prefix' = 'bronze-cdc-orders-dlq',
  'properties.transaction.timeout.ms' = '900000'
);

-- Generated layout: one block per table (depots, vans, orders); keep them identical.
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO lakehouse.bronze.depots_cdc
  SELECT d.op, d.`before`, d.`after`, d.snapshot, d.lsn, d.tx_id,
         CAST(d.commit_ts AS TIMESTAMP_LTZ(6)), CAST(d.capture_ts AS TIMESTAMP_LTZ(6)),
         d.kafka_partition, d.kafka_offset, CAST(d.ingest_ts AS TIMESTAMP_LTZ(6))
  FROM depots_decoded
  WHERE d.error_reason IS NULL;

  INSERT INTO lakehouse.bronze.dlq_events
  SELECT 'cdc.public.depots', d.kafka_partition, d.kafka_offset, CAST(d.ingest_ts AS TIMESTAMP_LTZ(6)),
         d.error_reason, d.error_detail, d.schema_id, d.raw_value
  FROM depots_decoded
  WHERE d.error_reason IS NOT NULL;

  INSERT INTO depots_dlq_topic
  SELECT d.raw_value,
         MAP['reason', CAST(d.error_reason AS BYTES),
             'source_topic', CAST('cdc.public.depots' AS BYTES),
             'partition', CAST(CAST(d.kafka_partition AS STRING) AS BYTES),
             'offset', CAST(CAST(d.kafka_offset AS STRING) AS BYTES),
             'ingest_ts', CAST(CAST(d.ingest_ts AS STRING) AS BYTES)]
  FROM depots_decoded
  WHERE d.error_reason IS NOT NULL;

  INSERT INTO lakehouse.bronze.vans_cdc
  SELECT d.op, d.`before`, d.`after`, d.snapshot, d.lsn, d.tx_id,
         CAST(d.commit_ts AS TIMESTAMP_LTZ(6)), CAST(d.capture_ts AS TIMESTAMP_LTZ(6)),
         d.kafka_partition, d.kafka_offset, CAST(d.ingest_ts AS TIMESTAMP_LTZ(6))
  FROM vans_decoded
  WHERE d.error_reason IS NULL;

  INSERT INTO lakehouse.bronze.dlq_events
  SELECT 'cdc.public.vans', d.kafka_partition, d.kafka_offset, CAST(d.ingest_ts AS TIMESTAMP_LTZ(6)),
         d.error_reason, d.error_detail, d.schema_id, d.raw_value
  FROM vans_decoded
  WHERE d.error_reason IS NOT NULL;

  INSERT INTO vans_dlq_topic
  SELECT d.raw_value,
         MAP['reason', CAST(d.error_reason AS BYTES),
             'source_topic', CAST('cdc.public.vans' AS BYTES),
             'partition', CAST(CAST(d.kafka_partition AS STRING) AS BYTES),
             'offset', CAST(CAST(d.kafka_offset AS STRING) AS BYTES),
             'ingest_ts', CAST(CAST(d.ingest_ts AS STRING) AS BYTES)]
  FROM vans_decoded
  WHERE d.error_reason IS NOT NULL;

  INSERT INTO lakehouse.bronze.orders_cdc
  SELECT d.op, d.`before`, d.`after`, d.snapshot, d.lsn, d.tx_id,
         CAST(d.commit_ts AS TIMESTAMP_LTZ(6)), CAST(d.capture_ts AS TIMESTAMP_LTZ(6)),
         d.kafka_partition, d.kafka_offset, CAST(d.ingest_ts AS TIMESTAMP_LTZ(6))
  FROM orders_decoded
  WHERE d.error_reason IS NULL;

  INSERT INTO lakehouse.bronze.dlq_events
  SELECT 'cdc.public.orders', d.kafka_partition, d.kafka_offset, CAST(d.ingest_ts AS TIMESTAMP_LTZ(6)),
         d.error_reason, d.error_detail, d.schema_id, d.raw_value
  FROM orders_decoded
  WHERE d.error_reason IS NOT NULL;

  INSERT INTO orders_dlq_topic
  SELECT d.raw_value,
         MAP['reason', CAST(d.error_reason AS BYTES),
             'source_topic', CAST('cdc.public.orders' AS BYTES),
             'partition', CAST(CAST(d.kafka_partition AS STRING) AS BYTES),
             'offset', CAST(CAST(d.kafka_offset AS STRING) AS BYTES),
             'ingest_ts', CAST(CAST(d.ingest_ts AS STRING) AS BYTES)]
  FROM orders_decoded
  WHERE d.error_reason IS NOT NULL;
END;

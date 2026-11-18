-- bronze_gps: registry-framed Avro GPS pings → Iceberg bronze (append-only, exactly-once).
-- Bad messages never stop the job (spec 003, ADR 0010): the source reads raw bytes, a Python
-- UDF decodes and validates them, and the statement set routes rejects to the dead-letter
-- table and topic. Catalog, checkpointing and Python settings come from _init_catalog.sql.
SET 'pipeline.name' = 'bronze_gps';

CREATE TEMPORARY FUNCTION decode_gps AS 'contracts.udf.decode_gps_udf' LANGUAGE PYTHON;

CREATE TEMPORARY TABLE gps_raw (
  `value`         BYTES,
  ingest_ts       TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset    BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = '${GPS_TOPIC}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id' = 'flink-bronze-gps',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'raw'
);

-- One ROW per message with everything the sinks need. Sinks read only `d`, so all three share
-- one identical expression and the planner decodes each message once (EXPLAIN: one PythonCalc).
CREATE TEMPORARY VIEW gps_decoded AS
SELECT decode_gps(`value`, ingest_ts, kafka_partition, kafka_offset, '${SCHEMA_REGISTRY_URL}') AS d
FROM gps_raw;

CREATE TEMPORARY TABLE gps_dlq_topic (
  `value`  BYTES,
  headers  MAP<STRING, BYTES> METADATA
) WITH (
  'connector' = 'kafka',
  'topic' = 'dlq.${GPS_TOPIC}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'format' = 'raw',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'bronze-gps-dlq',
  'properties.transaction.timeout.ms' = '900000'
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO lakehouse.bronze.gps_pings
  SELECT d.vehicle_id, CAST(d.event_ts AS TIMESTAMP_LTZ(6)), d.lat, d.lon, d.speed_kmh,
         d.heading_deg, CAST(d.ingest_ts AS TIMESTAMP_LTZ(6)), d.kafka_partition, d.kafka_offset
  FROM gps_decoded
  WHERE d.error_reason IS NULL;

  INSERT INTO lakehouse.bronze.dlq_events
  SELECT '${GPS_TOPIC}', d.kafka_partition, d.kafka_offset, CAST(d.ingest_ts AS TIMESTAMP_LTZ(6)),
         d.error_reason, d.error_detail, d.schema_id, d.raw_value
  FROM gps_decoded
  WHERE d.error_reason IS NOT NULL;

  INSERT INTO gps_dlq_topic
  SELECT d.raw_value,
         MAP['reason', CAST(d.error_reason AS BYTES),
             'source_topic', CAST('${GPS_TOPIC}' AS BYTES),
             'partition', CAST(CAST(d.kafka_partition AS STRING) AS BYTES),
             'offset', CAST(CAST(d.kafka_offset AS STRING) AS BYTES),
             'ingest_ts', CAST(CAST(d.ingest_ts AS STRING) AS BYTES)]
  FROM gps_decoded
  WHERE d.error_reason IS NOT NULL;
END;

-- Shared init for every Flink SQL job (passed with `sql-client.sh -i`).
-- ${VARS} are filled from env by submit.sh, so the same SQL runs locally and on AWS.
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '${CHECKPOINT_INTERVAL}';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'table.local-time-zone' = 'UTC';
-- Python UDFs (contracts/, ADR 0010): the client ships /opt/ct (contracts + schemas) to the cluster.
SET 'python.executable' = '/opt/venv/bin/python';
SET 'python.client.executable' = '/opt/venv/bin/python';
SET 'python.files' = '/opt/ct';

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

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

-- The catalog options come whole from the environment (infra/local/lakehouse.env locally,
-- infra/aws/lakehouse.env on AWS: REST + MinIO or Glue + S3, spec 009).
CREATE CATALOG lakehouse WITH ('type' = 'iceberg', ${ICEBERG_CATALOG_OPTIONS});

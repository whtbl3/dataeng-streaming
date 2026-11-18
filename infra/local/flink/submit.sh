#!/usr/bin/env bash
# Submit a Flink SQL job once: render ${VARS} from env, skip if a job with the same
# pipeline name is already RUNNING (so `make up` can be re-run safely).
# Usage: submit.sh <job.sql> <pipeline-name>
set -euo pipefail
SQL_FILE="$1"
JOB_NAME="$2"
REST="http://${FLINK_JOBMANAGER_HOST:-jobmanager}:8081"

job_running() {
  curl -fsS "$REST/jobs/overview" | grep -qo "\"name\":\"${JOB_NAME}\"[^{}]*\"state\":\"RUNNING\""
}

for _ in $(seq 1 60); do
  curl -fsS "$REST/overview" >/dev/null 2>&1 && break
  sleep 2
done

if job_running; then
  echo "job '${JOB_NAME}' already RUNNING; nothing to do"
  exit 0
fi

RENDERED="/tmp/$(basename "$SQL_FILE")"
envsubst < "$SQL_FILE" > "$RENDERED"
if grep -n '\${' "$RENDERED"; then
  echo "unrendered variables left in $SQL_FILE" >&2
  exit 1
fi

/opt/flink/bin/sql-client.sh -Drest.address="${FLINK_JOBMANAGER_HOST:-jobmanager}" -f "$RENDERED"

for _ in $(seq 1 30); do
  if job_running; then
    echo "job '${JOB_NAME}' is RUNNING"
    exit 0
  fi
  sleep 2
done
echo "job '${JOB_NAME}' did not reach RUNNING" >&2
exit 1

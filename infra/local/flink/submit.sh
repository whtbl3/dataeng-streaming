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

INIT_FILE="$(dirname "$SQL_FILE")/_init_catalog.sql"
render() {
  local out="/tmp/$(basename "$1")"
  envsubst < "$1" > "$out"
  if grep -n '\${' "$out"; then
    echo "unrendered variables left in $1" >&2
    exit 1
  fi
  echo "$out"
}
RENDERED_INIT="$(render "$INIT_FILE")"
RENDERED="$(render "$SQL_FILE")"

/opt/flink/bin/sql-client.sh -Drest.address="${FLINK_JOBMANAGER_HOST:-jobmanager}" \
  -i "$RENDERED_INIT" -f "$RENDERED"

for _ in $(seq 1 30); do
  if job_running; then
    echo "job '${JOB_NAME}' is RUNNING"
    exit 0
  fi
  sleep 2
done
echo "job '${JOB_NAME}' did not reach RUNNING" >&2
exit 1

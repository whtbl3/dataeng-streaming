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

# Where to resume from (spec 004, spec 005):
#   1. the savepoint `make down` took (stop_jobs.sh), if any;
#   2. otherwise the newest retained checkpoint of this job's earlier runs. Without HA a killed
#      JobManager does not recover its jobs, but their checkpoints stay on the volume
#      (RETAIN_ON_CANCELLATION), and a checkpoint holds the Kafka offsets and the Iceberg commit
#      together, so the job neither loses nor rewrites messages;
#   3. otherwise from scratch (Kafka group offsets, or a full re-read of an Iceberg source).
# FRESH_JOBS (used by `make silver-rebuild`) skips 1 and 2. Operators the SQL no longer has are
# skipped, so an edited job still starts.
SP_ROOT="${SAVEPOINT_ROOT:-/opt/flink/checkpoints/savepoints}"
CK_ROOT="${CHECKPOINT_ROOT:-/opt/flink/checkpoints}"
PENDING="$SP_ROOT/$JOB_NAME.pending"
JOBIDS="$SP_ROOT/$JOB_NAME.jobids" # this job's run ids, newest first
mkdir -p "$SP_ROOT"
touch "$JOBIDS"

# newest completed checkpoint of the newest run that has one; prints "<jid> <path>"
latest_checkpoint() {
  local jid chk
  while read -r jid; do
    [ -n "$jid" ] || continue
    chk="$(ls -d "$CK_ROOT/$jid"/chk-*/_metadata 2>/dev/null | sort -t- -k2 -n | tail -n 1)"
    if [ -n "$chk" ]; then
      echo "$jid file:$(dirname "$chk")"
      return
    fi
  done < "$JOBIDS"
}

RESUME=""
RESUME_JID=""
if [[ " ${FRESH_JOBS:-} " == *" $JOB_NAME "* ]]; then
  [ -f "$PENDING" ] && mv "$PENDING" "$SP_ROOT/$JOB_NAME.discarded"
  : > "$JOBIDS"
  echo "job '${JOB_NAME}': starting fresh, saved state discarded"
elif [ -f "$PENDING" ]; then
  RESUME="$(cat "$PENDING")"
  echo "job '${JOB_NAME}': resuming from savepoint $RESUME"
else
  read -r RESUME_JID RESUME <<< "$(latest_checkpoint)" || true
  [ -n "$RESUME" ] && echo "job '${JOB_NAME}': no savepoint; resuming from retained checkpoint $RESUME"
fi
# A job that reads an Iceberg table incrementally keeps its read position as a snapshot id. Expiry
# (maintenance, SNAPSHOT_RETENTION) removes snapshots older than the retention, so a job stopped
# longer than that cannot continue: Iceberg would fail with "Starting snapshot ... is not a parent
# ancestor" and the job would restart forever. Refuse up front and say how to recover (spec 005).
to_seconds() {
  local v="$1"
  case "$v" in
    *d) echo $(( ${v%d} * 86400 )) ;;
    *h) echo $(( ${v%h} * 3600 )) ;;
    *m) echo $(( ${v%m} * 60 )) ;;
    *s) echo "${v%s}" ;;
    *) echo "$v" ;;
  esac
}
if [ -n "$RESUME" ] && grep -q "'streaming' = 'true'" "$SQL_FILE"; then
  saved="${RESUME#file:}"
  age=$(( $(date +%s) - $(stat -c %Y "$saved/_metadata") ))
  limit="$(to_seconds "${SNAPSHOT_RETENTION:-7d}")"
  if [ "$age" -gt "$limit" ]; then
    echo "job '${JOB_NAME}' was stopped $((age / 3600)) h ago, longer than the snapshot retention" \
      "(${SNAPSHOT_RETENTION:-7d}): the Iceberg snapshot it would continue from may be gone." >&2
    echo "Rebuild it from bronze instead: make silver-rebuild" >&2
    exit 1
  fi
fi
if [ -n "$RESUME" ]; then
  {
    echo "SET 'execution.state-recovery.path' = '$RESUME';"
    echo "SET 'execution.state-recovery.ignore-unclaimed-state' = 'true';"
  } >> "$RENDERED_INIT"
fi

/opt/flink/bin/sql-client.sh -Drest.address="${FLINK_JOBMANAGER_HOST:-jobmanager}" \
  -i "$RENDERED_INIT" -f "$RENDERED"

# Keep disk flat: once the new run is RUNNING, drop the checkpoints of runs older than the one it
# resumed from (a restored run takes a full first checkpoint, so it never needs older files), and
# keep the two newest savepoints.
remember_run() {
  local new_jid keep=() jid
  new_jid="$(curl -fsS "$REST/jobs/overview" | python3 -c '
import json, sys
jobs = [j for j in json.load(sys.stdin)["jobs"] if j["name"] == sys.argv[1] and j["state"] == "RUNNING"]
print(max(jobs, key=lambda j: j["start-time"])["jid"] if jobs else "")
' "$JOB_NAME")"
  [ -n "$new_jid" ] || return 0
  keep=("$new_jid")
  [ -n "$RESUME_JID" ] && keep+=("$RESUME_JID")
  while read -r jid; do
    [ -n "$jid" ] || continue
    [[ " ${keep[*]} " == *" $jid "* ]] && continue
    rm -rf "${CK_ROOT:?}/$jid"
  done < "$JOBIDS"
  printf '%s\n' "${keep[@]}" > "$JOBIDS"
  # Runs that ended without a completed checkpoint (stopped with a savepoint, or never got far)
  # leave empty directories behind; no job can resume from them.
  for dir in "$CK_ROOT"/*/; do
    jid="$(basename "$dir")"
    [ "$jid" = "$(basename "$SP_ROOT")" ] && continue
    ls "$dir"chk-*/_metadata >/dev/null 2>&1 && continue
    cat "$SP_ROOT"/*.jobids 2>/dev/null | grep -qx "$jid" && continue
    rm -rf "${CK_ROOT:?}/$jid"
  done
  if [ -d "$SP_ROOT/$JOB_NAME" ]; then
    ls -dt "$SP_ROOT/$JOB_NAME"/savepoint-* 2>/dev/null | tail -n +3 | xargs -r rm -rf
  fi
}

for _ in $(seq 1 30); do
  if job_running; then
    [ -f "$PENDING" ] && mv "$PENDING" "$SP_ROOT/$JOB_NAME.used"
    remember_run
    echo "job '${JOB_NAME}' is RUNNING"
    exit 0
  fi
  sleep 2
done
echo "job '${JOB_NAME}' did not reach RUNNING" >&2
exit 1

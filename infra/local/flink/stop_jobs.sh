#!/usr/bin/env bash
# Stop every running job with a savepoint, so the next submit.sh resumes exactly where it stopped
# (`make down`, spec 004). Without HA, a JobManager that is simply killed discards its checkpoints;
# the jobs would then restart from Kafka group offsets, which can lag behind the last Iceberg commit
# and write some messages twice. A savepoint holds the Kafka offsets and the Iceberg commit together.
# Each savepoint path is written to savepoints/<job name>.pending, which submit.sh consumes.
set -euo pipefail
REST="http://${FLINK_JOBMANAGER_HOST:-jobmanager}:8081"
SP_ROOT="${SAVEPOINT_ROOT:-/opt/flink/checkpoints/savepoints}"
mkdir -p "$SP_ROOT"

if ! curl -fsS "$REST/overview" >/dev/null 2>&1; then
  echo "Flink is not running; nothing to stop"
  exit 0
fi

jobs="$(curl -fsS "$REST/jobs/overview" | python3 -c '
import json, sys
for j in json.load(sys.stdin)["jobs"]:
    if j["state"] == "RUNNING":
        print(j["jid"], j["name"])
')"

status=0
while read -r jid name; do
  [ -n "${jid:-}" ] || continue
  trigger="$(curl -fsS -X POST -H 'Content-Type: application/json' \
    -d "{\"targetDirectory\": \"file://$SP_ROOT/$name\", \"drain\": false}" \
    "$REST/jobs/$jid/stop" | python3 -c 'import json, sys; print(json.load(sys.stdin)["request-id"])')"
  location=""
  for _ in $(seq 1 90); do
    reply="$(curl -fsS "$REST/jobs/$jid/savepoints/$trigger")"
    location="$(python3 -c '
import json, sys
r = json.loads(sys.argv[1])
if r["status"]["id"] == "COMPLETED":
    op = r.get("operation", {})
    print(op.get("location") or "FAILED: " + str(op.get("failure-cause", {}).get("stack-trace", ""))[:300])
' "$reply")"
    [ -n "$location" ] && break
    sleep 2
  done
  case "$location" in
    file:*)
      echo "$location" > "$SP_ROOT/$name.pending"
      echo "job '$name' stopped with savepoint $location" ;;
    *)
      echo "job '$name': savepoint did not complete (${location:-timeout}); it will restart from Kafka offsets" >&2
      status=1 ;;
  esac
done <<< "$jobs"
exit $status

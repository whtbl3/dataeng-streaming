#!/usr/bin/env bash
# Rebuild silver from bronze (spec 004 AC8): cancel the silver jobs, drop and recreate the silver
# tables, then resubmit. The jobs start with empty state and read bronze from its first snapshot,
# so the result must equal the silver built live. Bronze is never touched.
# Usage (from the repo root, via `make silver-rebuild`): COMPOSE="docker compose ..." silver_rebuild.sh
set -euo pipefail
: "${COMPOSE:?set COMPOSE to the docker compose command}"
REST="${FLINK_REST:-http://localhost:8081}"
JOBS=(silver_gps silver_orders)

running_ids() {
  curl -fsS "$REST/jobs/overview" | python3 -c '
import json, sys
names = set(sys.argv[1:])
for j in json.load(sys.stdin)["jobs"]:
    if j["name"] in names and j["state"] in ("RUNNING", "CREATED", "RESTARTING", "INITIALIZING"):
        print(j["jid"])
' "${JOBS[@]}"
}

for jid in $(running_ids); do
  echo "cancelling job $jid"
  curl -fsS -X PATCH "$REST/jobs/$jid?mode=cancel" >/dev/null
done
for _ in $(seq 1 60); do
  [ -z "$(running_ids)" ] && break
  sleep 2
done
if [ -n "$(running_ids)" ]; then
  echo "silver jobs did not stop" >&2
  exit 1
fi

trino() { $COMPOSE exec -T trino trino "$@"; }
for table in gps_pings orders order_status_history; do
  trino --execute "DROP TABLE IF EXISTS lakehouse.silver.$table"
done
trino < infra/local/trino/init/004_silver.sql
# Sets the identifier fields again and submits the silver jobs; bronze jobs are left running.
FRESH_JOBS="${JOBS[*]}" $COMPOSE run --rm flink-submit
echo "silver rebuild submitted; it catches up from the first bronze snapshot"

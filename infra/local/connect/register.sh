#!/usr/bin/env bash
# Create or update the Debezium connector (PUT is idempotent), then wait until its task runs.
set -euo pipefail
CONNECT="${CONNECT_URL:-http://connect:8083}"
NAME=ct-orders

curl -fsS -X PUT -H 'Content-Type: application/json' \
  --data @/config/ct-orders.json "$CONNECT/connectors/$NAME/config" >/dev/null

state=""
for _ in $(seq 1 60); do
  state="$(curl -fsS "$CONNECT/connectors/$NAME/status" || true)"
  if echo "$state" | grep -q '"tasks":\[{"id":0,"state":"RUNNING"'; then
    echo "connector $NAME RUNNING"
    exit 0
  fi
  sleep 2
done
echo "connector $NAME not RUNNING: $state" >&2
exit 1

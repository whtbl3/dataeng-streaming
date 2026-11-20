#!/usr/bin/env bash
# `make dbt-ci` (spec 007 AC11, resolved question 5): dbt unit tests and SQL lint without the
# stack. Starts a throwaway Trino whose `lakehouse` catalog is in memory, creates the source
# tables empty, builds every model empty (types for the unit tests), runs the unit tests.
set -euo pipefail
cd "$(dirname "$0")/.."
NAME=ct-dbt-ci
PORT="${DBT_CI_PORT:-18089}"
WORK="$(mktemp -d)"
trap 'docker rm -f "$NAME" >/dev/null 2>&1 || true; rm -r "$WORK"' EXIT

mkdir -p "$WORK/catalog" "$WORK/profiles"
echo "connector.name=memory" > "$WORK/catalog/lakehouse.properties"
cp dbt/profiles.yml.example "$WORK/profiles/profiles.yml"
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -p "$PORT:8080" -v "$WORK/catalog:/etc/trino/catalog:ro" \
  trinodb/trino:484 >/dev/null
for _ in $(seq 1 90); do
  docker exec "$NAME" trino --execute "SELECT 1" >/dev/null 2>&1 && break
  sleep 2
done
docker exec -i "$NAME" trino < infra/local/dbt/ci_sources.sql >/dev/null

export DBT_PROFILES_DIR="$WORK/profiles" DBT_TARGET=ci DBT_TRINO_PORT="$PORT"
cd dbt
uv run --group dbt dbt run --empty --quiet
uv run --group dbt dbt test --select test_type:unit
uv run --group dbt sqlfluff lint models tests macros

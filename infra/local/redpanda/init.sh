#!/usr/bin/env bash
# Idempotent Redpanda setup (spec 002 topics + spec 003 contracts):
#   - source topics and their dead-letter topics `dlq.<topic>` (3 partitions, 7 days)
#   - FULL_TRANSITIVE compatibility on every value subject (ADR 0008)
#   - the GPS schemas from schemas/<subject>/v1.avsc, v2.avsc, ... registered in order
# CDC schemas are registered by the Debezium Avro converter; the mode set here protects them.
set -euo pipefail
BROKERS=(-X brokers=redpanda:9092)
REGISTRY=(-X registry.hosts=redpanda:8081)

for t in "${GPS_TOPIC:-gps.pings.v1}" cdc.public.depots cdc.public.vans cdc.public.orders; do
  for topic in "$t" "dlq.$t"; do
    rpk topic describe "$topic" "${BROKERS[@]}" >/dev/null 2>&1 \
      || rpk topic create "$topic" -p 3 -r 1 -c retention.ms=604800000 "${BROKERS[@]}"
  done
  rpk registry compatibility-level set "$t-value" --level FULL_TRANSITIVE "${REGISTRY[@]}"
done

for dir in /schemas/*/; do
  subject="$(basename "$dir")"
  for file in $(ls "$dir" | sort -V); do
    rpk registry schema create "$subject" --schema "$dir$file" --type avro "${REGISTRY[@]}"
  done
done

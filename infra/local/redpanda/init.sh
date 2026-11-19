#!/usr/bin/env bash
# Idempotent Redpanda setup (spec 002 topics + spec 003 contracts + spec 005 scale):
#   - source topics and their dead-letter topics `dlq.<topic>`: GPS 6 partitions (room for 6 parallel
#     readers), CDC 3; partitions are only ever added, never removed
#   - retention: 7 days or a size bound per partition, whichever comes first (NFR-5), set on every
#     run so existing topics get it too
#   - FULL_TRANSITIVE compatibility on every value subject (ADR 0008)
#   - the GPS schemas from schemas/<subject>/v1.avsc, v2.avsc, ... registered in order
# CDC schemas are registered by the Debezium Avro converter; the mode set here protects them.
set -euo pipefail
BROKERS=(-X brokers=redpanda:9092)
REGISTRY=(-X registry.hosts=redpanda:8081)
MIB=$((1024 * 1024))
SEVEN_DAYS_MS=604800000

# ensure_topic <topic> <partitions> <retention bytes per partition>
ensure_topic() {
  local topic="$1" parts="$2" bytes="$3" have
  if rpk topic describe "$topic" "${BROKERS[@]}" >/dev/null 2>&1; then
    have="$(rpk topic describe "$topic" -p "${BROKERS[@]}" | tail -n +2 | grep -c .)"
    if [ "$have" -lt "$parts" ]; then
      rpk topic add-partitions "$topic" --num "$((parts - have))" "${BROKERS[@]}"
    fi
  else
    rpk topic create "$topic" -p "$parts" -r 1 "${BROKERS[@]}"
  fi
  rpk topic alter-config "$topic" --set retention.ms="$SEVEN_DAYS_MS" \
    --set retention.bytes="$bytes" "${BROKERS[@]}"
}

GPS="${GPS_TOPIC:-gps.pings.v1}"
ensure_topic "$GPS" 6 $((1024 * MIB))
ensure_topic "dlq.$GPS" 6 $((128 * MIB))
for t in cdc.public.depots cdc.public.vans cdc.public.orders; do
  ensure_topic "$t" 3 $((256 * MIB))
  ensure_topic "dlq.$t" 3 $((128 * MIB))
done
for t in "$GPS" cdc.public.depots cdc.public.vans cdc.public.orders; do
  rpk registry compatibility-level set "$t-value" --level FULL_TRANSITIVE "${REGISTRY[@]}"
done

for dir in /schemas/*/; do
  subject="$(basename "$dir")"
  for file in $(ls "$dir" | sort -V); do
    rpk registry schema create "$subject" --schema "$dir$file" --type avro "${REGISTRY[@]}"
  done
done

# 0008: Avro + Schema Registry as data contracts
Date: 2026-11-17 · Status: Proposed (confirm the AWS side in feature 8)

## Context
Producers (the simulator, Debezium) and consumers (Flink) must agree on event shape, and
schema changes must not break running jobs.

## Decision
Avro schemas in `schemas/` are the contract, with BACKWARD compatibility enforced by the
registry and checked in CI. Locally, use the Redpanda Schema Registry (Confluent API).
On AWS, use AWS Glue Schema Registry (Flink `avro-glue` format, Glue converter in MSK Connect).

## Options considered
- **Avro + registry (chosen):** compact, typed, and evolution rules are enforced.
- **JSON + JSON Schema checked only in CI:** simpler, but no runtime enforcement and larger messages.
- **Protobuf:** fine too, but less native in Flink SQL and Debezium.

## Consequences
− Different serde formats locally (Confluent wire format) and on AWS (Glue). If this proves
  too costly in feature 8, fall back to a self-hosted Confluent-compatible registry on Fargate.
  That is why this ADR is Proposed.

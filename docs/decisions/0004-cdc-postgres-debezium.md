# 0004: Order events via Postgres + Debezium CDC
Date: 2026-11-17 · Status: Final

## Context
In real companies, orders live in an OLTP database owned by another team. Streaming
them without changing that application is the CDC pattern.

## Decision
The simulator writes orders to Postgres. Debezium (Kafka Connect, `pgoutput` logical
replication) publishes changes to `cdc.public.orders`. On AWS, this runs as RDS
PostgreSQL (`rds.logical_replication=1`) plus MSK Connect with the Debezium plugin.

## Options considered
- **Debezium (chosen):** industry standard, captures deletes and before/after images, and Flink reads it natively (`debezium-avro-confluent` format).
- **AWS DMS → Kinesis/MSK:** managed, but heavier and less common in job descriptions.
- **Simulator writes order events straight to Kafka:** simplest, but no CDC story and dual-write problems are hidden.

## Consequences
+ Interview topics: replication slots, snapshot vs streaming, the outbox pattern, changelog semantics.
− An inactive replication slot can fill the WAL disk; monitoring and a teardown step are required.

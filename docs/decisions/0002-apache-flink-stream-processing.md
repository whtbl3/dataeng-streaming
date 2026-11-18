# 0002: Apache Flink (SQL first) for stream processing
Date: 2026-11-17 · Status: Final

## Context
Alerts need event-time semantics, stateful joins (latest vehicle position × active
orders), deduplication with TTL, watermarks for late data, and exactly-once sinks.

## Decision
Use Apache Flink 1.20. Write jobs in **Flink SQL** where possible, and in PyFlink
(Table API + Python UDFs) for ETA logic. Run them in Docker locally and on Amazon
Managed Service for Apache Flink (MSF) on AWS.

## Options considered
- **Flink (chosen):** true streaming, the best support for event time and state, managed on AWS. It costs ~$0.11 per KPU-hour, with at least 1 KPU plus 1 orchestration KPU per app.
- **Spark Structured Streaming:** familiar API, but micro-batch latency, and arbitrary stateful logic is clumsier.
- **Plain consumers / Lambda:** simple, but watermarks, state and exactly-once would have to be hand-written.

## Consequences
+ The SQL jobs are readable in interviews and testable with a MiniCluster.
− PyFlink pins Python ≤ 3.11. Before feature 8, check which Flink runtime versions MSF supports.
− Packaging connector JARs (Kafka, Iceberg, JDBC) is required for MSF.

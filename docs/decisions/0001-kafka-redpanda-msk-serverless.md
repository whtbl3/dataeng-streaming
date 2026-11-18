# 0001: Kafka API — Redpanda locally, Amazon MSK Serverless on AWS
Date: 2026-11-17 · Status: Final

## Context
We need a durable, replayable event log for GPS pings, CDC changes and alerts. It must
run in Docker for $0 and map to an AWS managed service.

## Decision
Use the Kafka protocol. Locally, run Redpanda (single binary, built-in Schema Registry and console).
On AWS, use Amazon MSK Serverless with IAM authentication.

## Options considered
- **Kafka API (chosen):** most transferable skill, Debezium is native, and the client code is identical. MSK Serverless costs ~$0.75/h per cluster plus partition-hours.
- **Kinesis Data Streams:** cheapest on AWS and fully native, but local emulation (LocalStack) is unreliable with the Flink connector, and the CDC path needs DMS.
- **Apache Kafka in Docker locally:** closer to MSK, but heavier (needs a separate Schema Registry and Connect) and slower to start.

## Consequences
+ Replay and backfill by resetting consumer offsets.
− IAM SASL auth on MSK differs from the plaintext local listener; this is kept in config.
− MSK Serverless limits (e.g. max partitions, no tiered storage settings) must be checked during feature 8.
Reversal: switch the Flink source/sink connectors to Kinesis; the jobs keep their SQL logic.

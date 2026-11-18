# 0010: Dead-letter routing with raw-bytes sources and a Python decode UDF
Date: 2026-11-18 · Status: Final (spec 003)

## Context
A bad message must not stop a bronze job (spec 003, DRN-1). Flink SQL's `avro-confluent` and
`json` formats can only fail the job or silently drop a bad record; they cannot route it. The
rejected bytes and the reason must reach a dead-letter table and topic, and the good messages
must keep their exactly-once path into Iceberg.

## Decision
Each bronze source reads `format = 'raw'` bytes plus Kafka metadata. One PyFlink scalar UDF per
source (`contracts/udf.py`) decodes the Confluent wire format with the writer schema from the
registry, resolves it against the reader schema in `schemas/`, applies the GPS rules, and
returns **one ROW** holding the decoded fields, `error_reason` / `error_detail` / `schema_id`,
the Kafka metadata and, for rejects, the raw bytes. A STATEMENT SET sends `error_reason IS NULL`
rows to bronze and the rest to `bronze.dlq_events` (Iceberg, exactly-once) and `dlq.<topic>`
(Kafka, transactional). All sinks read only that ROW, so the planner reuses a single decode
(`EXPLAIN` shows one `PythonCalc`, the other branches marked `Reused`).

An unreachable registry is *not* a bad message: the UDF raises, the job fails over and retries
from its checkpoint, and nothing good is dead-lettered.

## Options considered
- **Raw source + Python UDF (chosen):** stays in Flink SQL, the rules are plain Python with unit
  tests, the same code is used by the producer, and Managed Flink supports PyFlink.
- **Java/Scala UDF or DataStream job:** faster, but adds a JVM build to a Python-only repo.
- **`ignore-parse-errors`:** keeps the job alive but loses bad records without trace.
- **Validation in a separate pre-processing job:** two hops, more state and more cost.

## Consequences
- \+ No message can stop a bronze job. Rejects are queryable with SQL and replayable from the topic.
- − A Python worker runs beside the JVM (taskmanager memory 1.5 → 2 GB). Each record crosses the
  JVM/Python boundary. That is fine for live load (about 40 events/s); the 100M backfill
  (≥ 20k events/s) is measured in feature 5. If it is too slow, tune the bundle size first,
  then move to a vectorized (pandas) UDF.
- − Bronze sources have no `event_ts` column before decoding, so they declare no watermark.
  Bronze is append-only and uses no event time; silver (feature 4) declares the watermark.
- − Sinks must keep reading only the UDF ROW. A sink that reads a source column directly
  breaks the sub-plan reuse and decodes every message once more (check with `EXPLAIN`).

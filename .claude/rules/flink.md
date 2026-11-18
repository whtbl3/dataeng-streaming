---
paths: ["flink/**"]
---
# Flink job rules
- Prefer Flink SQL; use PyFlink only for logic SQL cannot express (e.g. ETA UDFs).
- Every source table that has an `event_ts` column declares a WATERMARK on it. Bronze raw-bytes
  sources are the exception: they have no `event_ts` before decoding and use no event time
  (ADR 0010); silver declares the watermark. Never use PROCTIME() for business logic.
- Keyed state that can grow (dedup, latest position) must set a state TTL.
- Sinks must be exactly-once (Iceberg) or idempotent upserts (JDBC with a primary key).
- Connection settings come from config/env only, so the same job runs locally and on Managed Flink.
- Pure Python logic used only by Flink (haversine, ETA) lives in `flink/udf/`; logic shared with
  the producer (decoding, validation rules) lives in `contracts/`. Both have unit tests without Flink.
- Sinks in a statement set read only the decode UDF's ROW, so the decode runs once (ADR 0010).

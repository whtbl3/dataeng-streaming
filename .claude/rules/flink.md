---
paths: ["flink/**"]
---
# Flink job rules
- Prefer Flink SQL; use PyFlink only for logic SQL cannot express (e.g. ETA UDFs).
- Every source table declares a WATERMARK on `event_ts`. Never use PROCTIME() for business logic.
- Keyed state that can grow (dedup, latest position) must set a state TTL.
- Sinks must be exactly-once (Iceberg) or idempotent upserts (JDBC with a primary key).
- Connection settings come from config/env only, so the same job runs locally and on Managed Flink.
- Pure Python logic (haversine, ETA) lives in `flink/udf/` and has unit tests without Flink.

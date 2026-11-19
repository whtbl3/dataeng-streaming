---
paths: ["flink/**"]
---
# Flink job rules
- Prefer Flink SQL; use PyFlink only for logic SQL cannot express (e.g. ETA UDFs).
- Every source table that has an `event_ts` column declares a WATERMARK on it. Bronze raw-bytes
  sources are the exception: they have no `event_ts` before decoding and use no event time
  (ADR 0010); silver declares the watermark. A watermark needs TIMESTAMP(3) precision or less, so
  declare it on a computed `event_time AS CAST(event_ts AS TIMESTAMP_LTZ(3))` (ADR 0011).
  Never use PROCTIME() for business logic.
- Dedup and "latest" rank by data (`ingest_ts` + offset, `source_lsn`), never by processing order.
- Silver and serving sinks upsert on a key. For Iceberg, the key is the table's identifier fields,
  set by `infra/local/iceberg/set_identifier_fields.py`; a Flink DDL PRIMARY KEY is ignored.
- Keyed state that can grow (dedup, latest position) must set a state TTL.
- Sinks must be exactly-once (Iceberg append), or idempotent upserts (Iceberg identifier fields, JDBC primary key).
- Connection settings come from config/env only, so the same job runs locally and on Managed Flink.
- Pure Python logic used only by Flink (haversine, ETA) lives in `flink/udf/`; logic shared with
  the producer (decoding, validation rules) lives in `contracts/`. Both have unit tests without Flink.
- Sinks in a statement set read only the decode UDF's ROW, so the decode runs once (ADR 0010).

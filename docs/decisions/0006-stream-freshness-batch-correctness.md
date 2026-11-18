# 0006: Stream for freshness, dbt batch for correct KPIs
Date: 2026-11-17 · Status: Final

## Context
GPS events arrive late (up to 10 min), out of order, and duplicated. Alerts must be fast.
KPIs must be exact and must not depend on arrival order.

## Decision
- Flink computes alerts and ETAs per event with a short watermark (≈30 s). Events later than that still land in bronze/silver, but do not re-trigger old alerts.
- Silver tables are deduplicated by natural key (`vehicle_id`, `event_ts`) using Iceberg upserts.
- **Intraday run (every 5 min):** dbt rebuilds gold KPIs incrementally over a look-back
  window (last 2 hours) with `unique_key` merges, so normal late data is absorbed on the next run.
  Rows are marked `is_final = false`.
- **Nightly reconciliation ("close the books", 01:00 Asia/Ho_Chi_Minh):** dbt fully recomputes
  the previous local day's partitions from silver, sets `is_final = true`, and records the
  run in `gold.closed_days` (day, closed_at, late_events_absorbed). Events that arrive for a
  closed day are still stored in silver and counted in a `late_after_close` metric; they
  are applied only by an explicit `make reclose DAY=...` (an auditable restatement).

## Options considered
- **Hybrid + nightly close (chosen):** low-latency alerts, correct KPIs, and a clear
  point when numbers become final; easy to explain.
- **Hybrid with look-back window only:** misses events later than the window (e.g. 5 h offline).
- **Stream-only KPIs with long allowed lateness:** large state and complex retraction handling.
- **Batch-only:** correct but not real time, which is the original problem.

## Consequences
+ A test can prove that "late/duplicated input ⇒ identical gold KPIs".
+ Two clear service levels: *provisional* intraday KPIs (~5 min fresh) and *final* daily
  KPIs (correct even for events hours late, e.g. a van that was offline).
+ The nightly run is idempotent (it overwrites partitions), so it is safe to re-run.
− KPIs are eventually consistent; dashboards must show whether a number is provisional or final.
− One more scheduled job, and restating a closed day is a manual, logged action.

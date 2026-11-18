# 0009: Data volume tiers and the 100M-event benchmark
Date: 2026-11-18 · Status: Final

## Context
The live rate is small (200 vans × 1 ping / 5 s ≈ 40 events/s ≈ 3.5M events/day), so it
does not prove that the design scales. Tens to a hundred million events is not "big data"
by industry standards. It is still enough to need the same techniques: partitioning,
compaction, incremental models, backpressure, and throughput measurement.

## Decision
**Data tiers** (all from the same simulator, with a deterministic seed):
| Tier | Data | Purpose |
|---|---|---|
| `dev` | 20 vans × 1 day ≈ 350k pings | fast tests, CI e2e |
| `standard` | 200 vans × 30 days ≈ 100M pings, ~150k orders, ~750k CDC events | design target and main benchmark |
| `stress` | `standard` backfill + 2,000 vans live at the same time | backpressure and consumer lag |

- `make backfill DAYS=N` produces historical events (past `event_ts`) through **the same
  Kafka → Flink path** as live data, so replay and live use the same jobs (event time).
  Alerts are suppressed for events older than a configurable horizon.
- `make bench` records throughput, consumer lag, checkpoint duration, files per partition and
  disk use to `bench/results/<date>-<tier>-<host>.md`, together with the hardware it ran on.
- Iceberg GPS tables are partitioned by `day(event_ts)` + `bucket(16, vehicle_id)` and
  compacted on a schedule. Kafka retention is size-bounded so a backfill cannot fill the disk.
- Benchmarks run locally first. They can be repeated on the managed env later for comparison.

## Options considered
- **Tiers + backfill through the real path (chosen):** tests the actual pipeline; same code for live and replay.
- **Bulk-load history straight into Iceberg:** faster, but it skips Kafka and Flink, so nothing about the streaming path is proven.
- **Live rate only:** realistic, but proves nothing about scale.

## Consequences
+ Benchmark numbers carry the hardware they ran on, so they can be compared later.
+ Partitioning and compaction become necessary, not theoretical.
− A `standard` run takes time on a laptop. The throughput target is set after the first measurement (initial goal ≥ 20k events/s, i.e. 100M in < 90 min).
− Disk: about 10 GB in Kafka at peak (bounded retention) and 2–3 GB of Parquet.

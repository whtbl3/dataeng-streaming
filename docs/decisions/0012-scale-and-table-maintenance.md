# 0012: Scale by design: parallelism, cheaper events, bounded storage and table maintenance
Date: 2026-11-19 · Status: Final (spec 005)

## Context
The design targets about 100M GPS events (30 days at 200 vans, NFR-11). Adding machines does not
help when the design itself has a ceiling. Spec 005 looked at each layer for what stops it from
growing, and fixed that, without a large simulated load (proof uses the dev-size tests). Measured
on the dev stack (Flink 1.20.5, Iceberg 1.10.1, Trino 484, Redpanda 26.1, 8 cores, 10 GB WSL2).

## Decision
| Layer | Limit found | What we do |
|---|---|---|
| Kafka | 3 GPS partitions = at most 3 parallel readers | 6 partitions, keyed by `vehicle_id` (a van stays on one partition, in order); partitions are only ever added |
| Kafka | retention by time only: a big load can fill the disk | 7 days **or** a size bound per partition, whichever first: GPS 1 GiB, CDC 256 MiB, DLQ 128 MiB |
| Producers | many small requests | batches up to 512 KiB, `linger.ms` 20, `zstd` (simulator, Debezium, Flink DLQ sinks) |
| Flink | one Python call per message (ADR 0010) | vectorized UDF: one call per batch; same output (unit-tested), decode time −28 % (20k → 28k msg/s) |
| Flink | state on the heap, capped by RAM | RocksDB with incremental checkpoints; GPS jobs run with `GPS_PARALLELISM` (2 locally, up to 6) |
| Flink | a killed cluster lost its place (no HA) | `submit.sh` resumes from the newest retained checkpoint when there is no savepoint |
| Iceberg | each parallel writer writes a file per partition | `write.distribution-mode=hash`: one writer, one file per partition per commit |
| Iceberg | 16 buckets a day gave ~5 MB files (target 128–512 MB) | GPS partitioned by day only, files sorted by `(vehicle_id, event_ts)` when compacted |
| Iceberg | small files, upsert delete files, manifests, snapshots and metadata files pile up | the maintenance service (below), plus `write.metadata.delete-after-commit` (100 versions kept) |

**Maintenance service.** A small always-on container runs Iceberg procedures through Trino and
logs every run in `ops.maintenance_runs` (`make table-health` shows the result):
- every 30 min, compact silver: rewrite files under 128 MB, apply and drop delete files, sort;
- every 60 min, compact bronze;
- daily, merge manifests, expire snapshots older than 7 days, delete orphan files older than 7 days,
  and drop whole partitions past their retention (NFR-5: bronze GPS 90 days, bronze CDC and DLQ 1 year,
  silver 1 year).

A failed run is logged and retried at the next interval. `make maintain [TASK=…] [TABLE=…]` runs it now.

**Rules that come with it**
- Compaction runs while Flink upserts. The spike ran 147 back-to-back compactions on `silver.orders`
  during about 20k order changes: no failure, and silver equalled the latest version per order in
  bronze exactly. So upsert tables are compacted in every partition, not only cold ones.
- Silver GPS stays upsert (spec 005, resolved question 6 revised). The usual advice for event data
  is append + dedup, but Flink SQL cannot emit an insert-only stream that keeps the first arrival by
  data: `EXPLAIN CHANGELOG_MODE` shows `[I,UA,D]` for both a Top-1 and an event-time deduplicate.
  Delete files are therefore merged every 30 minutes instead of avoided.
- After a partition change, rewrite every old-spec data file before writers resume: an equality
  delete only applies to data files of its own partition spec (`005_scale.sql` does it in `trino-init`).
- Silver sources skip bronze `delete`/`overwrite` snapshots: those come only from retention, which
  must not reach silver (compaction's `replace` snapshots are always skipped).
- A job that reads a table incrementally may be stopped at most `SNAPSHOT_RETENTION` (7 days). After
  that its snapshot is gone and Iceberg fails ("Starting snapshot … is not a parent ancestor"); it
  does not skip data. `submit.sh` refuses such a resume up front and points to `make silver-rebuild`.

**Two deliberate departures from the usual advice**
- Python stays in the decode path (batched). A JVM decoder would be faster, but adds a Java build to
  a Python project; Flink's built-in Avro format would stop the job on a bad message (undoing spec 003).
- Bronze GPS stays partitioned by event time, not arrival time. Events are at most about 2 h late
  (spec 004), so late rows touch only today's or yesterday's partition.

**Mapping to AWS** (feature 9–10, by configuration): Glue table optimizers do compaction, snapshot
retention and orphan deletion; MSK topic configs carry the same retention bounds.

## Options considered
- **Maintenance through Trino on a schedule (chosen):** stdlib Python, no new engine; the same
  procedures exist on Athena/Glue.
- **Spark procedures (`rewrite_data_files`, `rewrite_position_delete_files`):** the most complete
  set, but a Spark runtime (about 1 GB) only for maintenance.
- **Flink's own small-file compaction:** only for appends, and it does not apply delete files.
- **No maintenance, bigger commits instead:** fewer files, but silver would be minutes staler and
  delete files would still pile up.

## Consequences
- \+ Disk and file counts stay flat on a stack left running for days (`make table-health`).
- \+ Scaling out is configuration: more partitions, `GPS_PARALLELISM`, more task slots.
- − Between compactions, reads of upsert tables apply delete files (at most 30 min of them).
- − A reader stopped longer than 7 days needs a rebuild from bronze.
- − Data tiers, `make backfill`, `make bench` and the `stress` run were dropped from this feature
  (ADR 0009, partly superseded); throughput at 100M is reasoned, not measured.
- Reverse: stop the `maintenance` service; table properties and layout are metadata and can be set back.

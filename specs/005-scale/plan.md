# Plan 005: Scale (parallelism, cheaper per-event work, table maintenance, bounded storage)
From: spec.md (Approved 2026-11-19) · Approved by: project owner (2026-11-19)
## Context
Spec `specs/005-scale/spec.md` (Approved 2026-11-19, 15 ACs, 6 resolved questions). The design targets
~100M GPS events, but today the GPS topic has 3 partitions, decode crosses into Python row by row,
state lives on the heap, silver upserts write an equality delete per row into 16 tiny buckets a day,
and nothing ever compacts, expires or bounds storage. No large simulated load: proof uses dev-size
e2e tests. On approval this becomes `specs/005-scale/plan.md` + `tasks.md`.

## What reading the code found (changes the plan, not the spec)
1. **AC2 is mostly done already.** `simulator/producer.py:make_producer` already sets `linger.ms=20`
   and `compression.type=lz4`; the key is `vehicle_id` (`producer.py:107`). Work left: `zstd` +
   `batch.size` for the simulator, producer compression for Debezium (`ct-orders.json`), and the test.
2. **Append for silver GPS makes compaction safe.** Rewriting files while Flink commits equality
   deletes can conflict or, if the rewrite gets a newer sequence number, let old rows reappear. With
   silver GPS append-only, the only upsert tables left (`silver.orders`, `order_status_history`) are
   small, so they are compacted only in *cold* partitions, and append tables in any partition.
3. **Append needs a way to remove a late copy exactly.** A replayed copy can be byte-identical, so a
   DELETE by value would remove both. Silver GPS gains `kafka_partition`/`kafka_offset` (lineage), and
   the dedup DELETE keeps the minimum `(ingest_ts, kafka_partition, kafka_offset)` per key: one
   atomic statement, position deletes only (see the spike outcome for why not `"$path"`).
4. **Retained checkpoints already exist on disk** (`RETAIN_ON_CANCELLATION`), but nothing uses them
   after a crash. `submit.sh` resumes from the latest retained checkpoint when there is no savepoint.
   This also closes the bronze-duplicate gap ADR 0011 left open (lost checkpoint without HA).
5. **Iceberg properties Trino cannot set** (distribution mode, metadata cleanup) go through the REST
   catalog, like `infra/local/iceberg/set_identifier_fields.py`. That script is generalised.

## Spike outcome (T1, 2026-11-19)
Run on the dev stack (Flink 1.20.5, Iceberg 1.10.1, Trino 484, Redpanda 26.1).
- **S1 compaction vs live upserts: safe in every partition.** 147 back-to-back
  `optimize(file_size_threshold => '128MB')` runs on `silver.orders` while about 20k order changes
  streamed in: 0 failed, the Flink job had 0 exceptions and 0 failed checkpoints, and silver equalled
  Top-1-by-LSN from bronze exactly (14,426 orders, 0 missing, 0 extra, 0 duplicate keys). `optimize`
  applies and removes equality and position deletes (9 delete files → 0). **Change:** upsert tables are
  compacted in all partitions, not cold ones only (R1 fallback no longer needed).
- **S1 other procedures.** `optimize_manifests` works (5 manifests → 1). `sorted_by` works.
  Session properties `expire_snapshots_min_retention` / `remove_orphan_files_min_retention` let tests
  expire with `0s`. `extra_properties` rejects `write.metadata.*` keys, so those go through the REST
  catalog as planned. Snapshot summaries omit `added-data-files` on empty commits (use `element_at`).
- **S1 dedup by `"$path"` is wrong.** Two identical copies in one file share `(key, "$path")`, so the
  DELETE removed the keeper too (317 keys lost on a scratch table). **Change:** the dedup DELETE
  identifies the keeper by `(vehicle_id, event_ts, ingest_ts, kafka_partition, kafka_offset)`, which
  separates every real copy (each has its own Kafka offset). Byte-identical copies (the same bronze row
  read twice) cannot be separated by any predicate, so they are prevented instead: `submit.sh` refuses
  to start `silver_gps` without state (no savepoint, no retained checkpoint) on a non-empty table unless
  it is in `FRESH_JOBS` (`make silver-rebuild`). `silver-status` reports identical copies; expected 0.
- **S2 batched decode: 28 % less decode time.** 172,820 pings, parallelism 1, the same mini-cluster:
  read only 9.3 s (startup + Kafka); row UDF 17.6–18.0 s; vectorized UDF 15.4–15.5 s. Decode alone:
  about 8.5 s → 6.1 s (≈ 20k → 28k messages/s). The rest is fastavro per row. Worth it, but not a
  10× change; ADR 0010 records both numbers. A vectorized UDF cannot return `TIMESTAMP_LTZ` inside a
  ROW, so its result uses `TIMESTAMP(3)` (UTC, the session time zone) and the SQL casts back to LTZ.
  A constant argument arrives as a scalar, not a Series.
- **S3 resume from a retained checkpoint works.** `docker compose kill jobmanager taskmanager` left
  `chk-359` on the volume. Resubmitting `bronze_gps` from it gave exactly one row per message for pings
  sent before the kill and pings sent while Flink was down (2,884 and 2,884, 0 duplicates).
  Retested with RocksDB incremental checkpoints in T3.
- **S4 write distribution** is checked directly by the AC11 test in T5 (no separate spike).

## Departures from the plan (build, 2026-11-19)
- **Silver GPS stays upsert** (spec resolved question 6, revised with the project owner). `EXPLAIN
  CHANGELOG_MODE` on the silver GPS dedup: a data-ordered Top-1 (`Rank`, AppendFastStrategy) and an
  event-time `Deduplicate(keep=FirstRow, order=ROWTIME)` both emit `[I,UA,D]`, so an append sink
  cannot take them. Dropped with it: findings 2 and 3 (lineage columns, the dedup task and its
  DELETE), the `submit.sh` guard against starting without state, AC14, and the `silver-status`
  change. Silver GPS gets its identifier fields from `table_admin.py` again.
- **Partition change needs an immediate full compaction.** An equality delete applies only to data
  files with the same partition spec (Iceberg spec, "Scan planning"). After `silver.gps_pings` drops
  `bucket(16)`, a late copy's delete in the new spec would not reach the old row. So `005_scale.sql`
  rewrites every old-spec file right after the ALTER; it runs in `trino-init`, before `flink-submit`
  resumes the jobs, so nothing writes in between. A test checks that every data file has the current
  spec id.
- `set_identifier_fields.py` became `table_admin.py` (identifier fields + write properties +
  `has-rows`), configured in one place (`FLINK_TABLES`, `TABLE_PROPERTIES`, `UPSERT_KEYS`).
- The vectorized UDF returns `TIMESTAMP(3)` (UTC) for its timestamps; the SQL casts as before.
- Spec 001 tests that hard-coded 3 GPS partitions now expect 6 (spec 005 AC1 changed the requirement).
- **AC13 found no silent skip.** With intermediate bronze snapshots expired, the resumed silver job
  fails loudly ("Starting snapshot (exclusive) … is not a parent ancestor of end snapshot …") and
  restarts forever; with only one commit in between it continues correctly. `submit.sh` therefore
  refuses a resume older than `SNAPSHOT_RETENTION` up front (savepoint or checkpoint age) and points
  to `make silver-rebuild`, instead of watching the job fail.
- Silver sources set `streaming-skip-delete-snapshots` / `-overwrite-snapshots`, so retention deletes
  on bronze never reach silver (not in the original plan).
- An idle Flink Iceberg sink commits an empty snapshot only every 10th checkpoint; the AC12 test
  sends pings to force commits.
- The explainer page (`docs/design/system-design-walkthrough.html`) was not updated: the file has
  uncommitted edits made outside this session, left untouched.

## Design
**Kafka** (`infra/local/redpanda/init.sh`)
- GPS topic and its DLQ: 6 partitions (`rpk topic add-partitions` when the topic exists: idempotent).
- Size bounds per partition: GPS 1 GiB, CDC 256 MiB, DLQ 128 MiB, plus `retention.ms` 7 d. Set with
  `rpk topic alter-config` on every run, so existing topics get them too.
- Flink Kafka sources set `scan.topic-partition-discovery.interval` (1 min) so new partitions are read.

**Flink**
- `state.backend.type: rocksdb`, `execution.checkpointing.incremental: true`, `state.checkpoints.num-retained: 2`.
- GPS job parallelism from env `GPS_PARALLELISM` (local default 2) via `SET 'parallelism.default'`
  in `bronze_gps.sql` and `silver_gps.sql`; slots 4 → 6; taskmanager 1900m → 2400m (`mem_limit` 2.5g).
- Decode in batches: a pure `contracts.decode.decode_gps_batch(values, …) -> list` (no pandas, unit
  tested), wrapped by a vectorized (pandas) UDF in `contracts/udf.py`. Same for CDC only if the spike
  shows a gain worth it. Spike decides; fallback below.
- `submit.sh`: record `job name → job id` at submit (`$SP_ROOT/<name>.jobid`); with no savepoint and
  not in `FRESH_JOBS`, resume from the newest `chk-*/_metadata` of that job id; after RUNNING, delete
  checkpoint dirs of older job ids of the same name.
- `silver_gps.sql`: plain append (no upsert hint), adds `kafka_partition`, `kafka_offset`.

**Iceberg tables** (`infra/local/trino/init/005_scale.sql`, idempotent ALTERs; `004_silver.sql` gets
the new shape for fresh installs)
- `silver.gps_pings`: partition `day(event_ts)` only, `sorted_by = ARRAY['vehicle_id','event_ts']`,
  add `kafka_partition`, `kafka_offset`. `bronze.gps_pings`: `sorted_by` the same (stays `day(event_ts)`;
  event time vs arrival time is a documented trade-off in ADR 0012).
- REST properties for every Flink-written table (`infra/local/iceberg/table_admin.py`, replaces
  `set_identifier_fields.py`, same stdlib + `call=` test seam): `write.distribution-mode=hash`,
  `write.metadata.delete-after-commit.enabled=true`, `write.metadata.previous-versions-max=100`,
  plus identifier fields for the two upsert tables. Run by `flink-submit` before the jobs.

**Maintenance service** (`infra/local/maintenance/maintain.py`, stdlib only, Trino HTTP API)
- New compose service `maintenance` (`python:3.11-slim`, ~40 MB), loop with per-task intervals from env:
  | Task | Tables | Every | Statement |
  |---|---|---|---|
  | compact | silver GPS and upsert tables (all partitions, S1) | 30 min | `EXECUTE optimize(file_size_threshold => '128MB') WHERE …` |
  | compact | bronze GPS, CDC, DLQ (all partitions) | 60 min | same |
  | manifests | all | daily | `EXECUTE optimize_manifests` |
  | expire | all | daily | `EXECUTE expire_snapshots(retention_threshold => '7d')` |
  | orphans | all | daily | `EXECUTE remove_orphan_files(retention_threshold => '7d')` |
  | retention | bronze GPS 90 d, bronze CDC/DLQ 1 y, silver 1 y (NFR-5) | daily | partition-aligned DELETE |
- Each run writes a row to `lakehouse.ops.maintenance_runs` (task, table, started, finished, status,
  detail). A failed task is logged and retried next interval; it never stops the loop.
- `make maintain [TASK=…]` runs once now (`docker compose run --rm maintenance --once`).
- `make table-health` (AC10): per table data files, delete files, avg file MB, snapshots (from
  `$files`, `$snapshots`) and last run per task.

**Snapshot expiry vs streaming readers (AC13).** Rule (ADR 0012, `flink.md`): a job that reads a table
incrementally may be stopped at most 7 days. If longer, its snapshot is gone and the Iceberg source
fails; `submit.sh` detects the failed job, prints the cause and the fix (`make silver-rebuild`) and exits
non-zero, instead of a silent skip.

## Files that change
- **New:** `infra/local/maintenance/{maintain.py,health.sql}`, `infra/local/trino/init/005_scale.sql`,
  `infra/local/iceberg/table_admin.py` (replaces `set_identifier_fields.py`),
  `docs/decisions/0012-scale-and-table-maintenance.md`, `tests/unit/{test_maintain,test_table_admin,
  test_decode_batch}.py`, `tests/integration/{test_scale,test_maintenance}.py`, `specs/005-scale/{plan,tasks}.md`.
- **Changed:** `infra/local/redpanda/init.sh`, `infra/local/docker-compose.yml` (Flink props, slots, TM
  memory, `GPS_PARALLELISM`, `maintenance` service, `flink-submit` command), `infra/local/flink/submit.sh`,
  `flink/jobs/{bronze_gps,silver_gps,bronze_cdc}.sql`, `contracts/{decode,udf}.py`,
  `infra/local/trino/init/004_silver.sql`, `infra/local/trino/silver_status.sql` (dedup removed count),
  `infra/local/connect/ct-orders.json`, `simulator/producer.py`, `Makefile`, `tests/unit/test_identifier_fields.py`
  (→ `test_table_admin.py`), `tests/integration/test_silver*.py` (append semantics).
- **Docs:** ADR 0009 (partly superseded), 0010 (batched decode), 0011 (silver GPS append, RocksDB,
  checkpoint resume), `docs/intent.md` (success measure), `docs/requirements.md` NFR-5/NFR-11,
  `docs/project-spec.md`, `docs/roadmap.md`, `README.md`, the Flink rules, explainer status.

## Order of work
1. **Spike (half a day), results into plan.md:**
   - S1 Trino 484: `optimize` on a table with equality deletes, while `silver_orders` commits (checksum);
     `optimize_manifests`; `sorted_by`; session props `expire_snapshots_min_retention`; DELETE on `"$path"`.
   - S2 Vectorized UDF in Flink 1.20: BYTES in, ROW out; decode time for the same 100k messages, row vs batch.
   - S3 Kill the jobmanager (`docker kill`), resubmit from the newest retained checkpoint; bronze exact.
   - S4 Parallelism 2 + `write.distribution-mode=hash`: one file per partition per commit.
2. Kafka partitions, size bounds, producer settings (AC1, AC2, AC9).
3. RocksDB + incremental checkpoints + checkpoint resume in `submit.sh` (AC4, finding 4).
4. Batched decode (AC3).
5. Silver GPS append + layout + REST properties (AC5 layout, AC11, AC12 properties, AC14 schema).
6. Maintenance service, `make maintain`, `make table-health` (AC5–AC8, AC10, AC12, AC14).
7. Expiry vs readers (AC13); memory (AC15).
8. Docs and ADR 0012; `make check`, `make e2e`, `make e2e-restart`; spec Implemented; PR.

## Risks and rollback
- **R1 `optimize` vs concurrent equality deletes** (S1). Fallback already in the design: upsert tables
  compact cold partitions only. If even that conflicts, the task retries next run (logged).
- **R2 Batched decode gives no gain** (S2): the Python cost is fastavro per row, not the call. Fallback:
  keep the row UDF, tune `python.fn-execution.bundle.size` / `arrow.batch.size`; record both numbers in
  ADR 0010; AC3 still holds (same output).
- **R3 Resume from retained checkpoint fails** (S3), e.g. incremental RocksDB files shared across job ids.
  Fallback: keep savepoint-only resume; silver GPS dedup cleans replays; bronze gap stays documented.
- **R4 Memory** with 6 slots, RocksDB and 2 Python workers per GPS job. Measured by AC15; fallback
  `GPS_PARALLELISM=1` locally (AC1 still proven by partition count + config).
- **R5 Adding partitions moves keys**: pings of a van sent across the switch can land on two
  partitions once. Silver ranks by data, so rows are unchanged; AC1 test checks pings sent after.
- **R6 Iceberg REST fixture (SQLite, 1 client)**: maintenance adds commits. Intervals keep it low;
  a busy commit is retried by Iceberg.
- **Rollback:** revert the branch; `docker compose stop maintenance`; partitions cannot be reduced
  (harmless); table property and partition changes are metadata-only and can be set back with ALTER.

## Proof
| AC | Test |
|---|---|
| AC1 partitions, key affinity, parallel readers | e2e `test_gps_topic_has_6_partitions_and_a_van_stays_on_one`, `test_gps_jobs_run_with_configured_parallelism` (REST vertex parallelism) |
| AC2 batched + compressed, same messages | unit producer config; e2e bronze count/values equal for a prefix (existing `test_exactly_n_rows`) + topic `compression` check |
| AC3 batch decode = row decode | unit `test_decode_batch.py` (mixed valid/invalid, same rows and reasons); spec 003 e2e unchanged |
| AC4 RocksDB hard stop | restart e2e (spec 004 AC7) with RocksDB; `test_jobmanager_kill_resumes_from_checkpoint` (bronze exact) |
| AC5 fewer files read | e2e: files whose vehicle_id/day bounds cover one van, before vs after `make maintain`, same rows |
| AC6 compaction keeps rows | e2e checksum + file/delete counts before and after |
| AC7 maintenance during live writes | e2e: produce while `maintain` loops 3×; silver equals expected; runs logged ok or retried |
| AC8 expiry, orphans, retention | e2e with session min-retention lowered: old snapshots gone, planted orphan gone, 100-day-old rows dropped |
| AC9 size bound | e2e on a small test topic (1 MiB bound): size stays ≤ bound + one segment; real topics' configs checked |
| AC10 table health | e2e `make -s table-health` matches direct `$files`/`$snapshots` counts |
| AC11 one writer per partition | e2e: snapshot summary `added-data-files` == `changed-partition-count` |
| AC12 metadata bounded | e2e: `$metadata_log_entries` ≤ 101 and old metadata files deleted; `$manifests` bounded after `optimize_manifests` |
| AC13 expiry vs stopped reader | restart e2e: stop `silver_gps`, expire all bronze snapshots, resubmit → clear error + non-zero; `silver-rebuild` recovers |
| AC14 | dropped (silver GPS stays upsert, see departures) |
| AC15 memory | existing memory e2e with maintenance running, < 6 GB |

## Verification
- `make check` (unit: decode batch, table admin, maintenance task SQL builders).
- `make clean && make up`, `make demo` for a few minutes, `make maintain`, `make table-health`:
  silver GPS has one file per day after compaction, delete files 0 on GPS, metadata log ≤ 101.
- `make e2e`, `make e2e-restart`, `make stack-mem` < 6 GB.

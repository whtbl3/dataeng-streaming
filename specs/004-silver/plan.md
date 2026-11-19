# Plan 004: Silver (dedup, event-time watermarks, chaos mode)
From: spec.md (Approved 2026-11-19) · Approved by: project owner (2026-11-19)

## Context
Spec `specs/004-silver/spec.md` (Approved 2026-11-19). Bronze keeps duplicates, late events and every
order version. This feature adds three silver tables built by Flink from bronze, plus a simulator
chaos mode that sends the same day dirty in known amounts. Together they prove that dirty input gives
identical silver. 

## Three refinements of the resolved questions (approve with this plan)
These come out of reading the code. Each one keeps an acceptance criterion true that would otherwise break.
1. **The simulator sets the Kafka record time to the simulated send time.** In history mode it
   produces 2 h of pings in a few seconds, so the broker time would make every ping look 0–2 h late.
   With the record time set to `event_ts + chaos delay` (a clean ping: `event_ts`), `ingest_ts` means
   "when the device sent it" in both modes, and lateness is real (AC3). In real time mode the two are
   the same. The spec 001 check `ingest_ts >= event_ts` still holds, and so does the DLQ rule
   `event_ts_in_future`: a corrupted ping is still 10 min ahead of the wall clock.
2. **Late = lateness > 30 s, taken from the data, not from the live watermark.** A flag based on
   `CURRENT_WATERMARK()` depends on the order Flink reads splits in, so a rebuild (AC8) could flag
   different rows. `lateness_s = ingest_ts − event_ts` (seconds; see the spike outcome) and `is_late = lateness_s > 30` give the
   same result on every run. The 30 s is the watermark allowance from ADR 0006. The silver GPS source
   still declares that 30 s watermark for later event-time operators (feature 6). See risk R1.
3. **Among copies of one ping, the first arrival wins (smallest `ingest_ts`), not the last write**
   (this changes resolved question 6). Last-write-wins would change `ingest_ts` and `lateness_s` when
   a copy comes back hours later, which breaks AC4. A Top-1 per key ordered by `ingest_ts`, with state
   TTL 24 h, emits each key once. The upsert sink on the primary key (resolved question 2b) stays as
   the backstop: a copy that arrives after the TTL still cannot create a second row, it can only
   refresh the arrival columns. `ingest_ts` and `lateness_s` then change for that row, and
   `make silver-status` counts it. ADR 0011 documents both layers.

## Spike outcome (T4, 2026-11-19)
Tried on the running stack with a throwaway `spike` schema (dropped afterwards):
- **Watermark:** an `iceberg` connector table accepts a WATERMARK, but only on `TIMESTAMP_LTZ(3)` or
  less. Bronze stores `TIMESTAMP_LTZ(6)`, so the source adds a computed column
  `event_time AS CAST(event_ts AS TIMESTAMP_LTZ(3))` and the watermark goes on that column.
- **Upsert key:** the sink does **not** take the key from the DDL `PRIMARY KEY` when the table already
  exists. It fails with "Equality field columns shouldn't be empty". The key comes from the Iceberg
  table's identifier fields, which Trino cannot set. Fallback R1 is used, with no new dependency:
  `infra/local/iceberg/set_identifier_fields.py` (stdlib only) adds a schema with
  `identifier-field-ids` through the REST catalog API. It is idempotent, and `flink-submit` runs it
  before the silver jobs. Key columns are `NOT NULL` in the Trino DDL, because Iceberg requires
  identifier fields to be required.
- **Upsert works:** with `'upsert-enabled' = 'true'`, bronze had 2,682 rows and 2,662 distinct keys,
  and silver came out at 2,662. Cancelling the job and resubmitting it without state reprocessed all
  of bronze, and silver still had 2,662 rows. Every write adds an equality-delete file (31 data files
  and 31 delete files per pass), which confirms R2.
- **Lateness unit:** Flink 1.20 does not parse `TIMESTAMPDIFF(MILLISECOND, …)`, so the column is
  `lateness_s` (seconds) and `is_late = lateness_s > 30`. Pings sit on 5 s ticks, so seconds are enough.

## Design
**Tables** (`infra/local/trino/init/004_silver.sql`). Created in Trino, Iceberg format v2, because
Flink DDL cannot declare partition transforms (same reason as `001_bronze.sql`).
| Table | Columns (beyond the source fields) | Primary key (upsert) | Partition |
|---|---|---|---|
| `silver.gps_pings` | `ingest_ts`, `lateness_s`, `is_late` | `(vehicle_id, event_ts)` | `day(event_ts)`, `bucket(16, vehicle_id)` |
| `silver.orders` | flattened `after` row + `last_lsn`, `last_commit_ts` | `(order_id, created_at)` | `month(created_at)` |
| `silver.order_status_history` | `order_id, status, status_ts, source_lsn, commit_ts` | `(order_id, status, status_ts)` | `day(status_ts)` |

Iceberg upsert requires the partition source columns in the key. `created_at` never changes per order,
and `status_ts` is set once per status, so adding them changes no uniqueness. The requirements
(section 4) are updated to match.

**Job `silver_gps`** (`flink/jobs/silver_gps.sql`, pure SQL, no Python):
- Streaming Iceberg read of `bronze.gps_pings`: `starting-strategy = TABLE_SCAN_THEN_INCREMENTAL`,
  `monitor-interval = 10s`. It is declared through a temporary `connector = 'iceberg'` table so that
  it can carry `WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND`.
- `ROW_NUMBER() OVER (PARTITION BY vehicle_id, event_ts ORDER BY ingest_ts, kafka_partition, kafka_offset) = 1`.
- Computes `lateness_s` and `is_late`.
- Writes upserts through a temporary iceberg-connector sink table that declares `PRIMARY KEY … NOT ENFORCED`.
- `table.exec.state.ttl = 24 h`.

**Job `silver_orders`** (`flink/jobs/silver_orders.sql`): one streaming read of `bronze.orders_cdc`, and a
STATEMENT SET with two sinks:
- `silver.orders`: Top-1 per `order_id` by `source_lsn DESC`, so an older change never wins (AC5).
  State is one row per order, TTL 7 days.
- `silver.order_status_history`: built from each version's `after` image.
  - Every non-null `created_at` / `assigned_at` / `out_for_delivery_at` / `delivered_at` / `failed_at`
    gives one `(order_id, status, status_ts)`, unnested with a `VALUES` cross join.
  - Then Top-1 per key by `source_lsn ASC`, which keeps the commit that first reached the status.
  - A snapshot read (`op = r`) of an order that is already delivered still yields its full history.
  - Duplicates collapse onto the key.

**Chaos** (`simulator/chaos.py`, pure, no I/O):
- `Chaos(config, seed).apply(stream)` takes the `merged_stream` tuples and yields `(send_ts, kind, item)`
  in send-time order.
  - Order events pass through unchanged.
  - Pings are held in a heap until the stream clock reaches their send time.
- Per-ping decisions are keyed by `splitmix64(van, epoch)` (reusing `_splitmix64` from `simulator/day.py`),
  so they do not depend on stream order, and the distinct pings equal the clean run (AC1).
- Kinds:
  - `duplicate`: 1–3 extra copies, +1–30 s.
  - `delayed`: 90% at 1–10 min, 10% at 10–120 min.
  - `jitter`: +0–10 s, which reorders pings across the 5 s ping interval.
  - `offline`: one 2–15 min window per van per day; its pings are flushed at reconnect, 50 ms apart.
- `counts` per kind, for the CLI line and the tests.
- CLI:
  - `--chaos` sets the defaults: 5% duplicate, 5% delayed, 10% jitter, offline on.
  - `--dup-rate`, `--delay-rate`, `--max-delay-min`, `--jitter-rate`, `--offline` override them.
  - `--late-rate` → `--late-delivery-rate` (resolved question 5).

**Producer** (`simulator/producer.py`):
- `send_raw(..., timestamp_ms)` passes `timestamp=` to `confluent_kafka.Producer.produce`.
- `run_day` sends at `send_ts`; in real time mode it sleeps until `send_ts`.
- `ProducerLike` and the test fakes gain the `timestamp` keyword.

**Operations:**
- `make silver-status`: per table and day, bronze rows, silver rows, duplicates removed, late rows,
  and conflicting copies (same key, different values).
- `make silver-rebuild`: cancel the silver jobs, drop and recreate the silver tables, resubmit (AC8).
- `flink-submit` also submits `silver_gps` and `silver_orders`, which brings the 4 slots to 4 in use.
- Taskmanager memory goes from 1900m to 2300m if AC11 needs it.

## Files that change
- **New:**
  - `simulator/chaos.py`
  - `flink/jobs/silver_gps.sql`, `flink/jobs/silver_orders.sql`
  - `infra/local/trino/init/004_silver.sql`
  - `infra/local/trino/silver_status.sql`
  - `docs/decisions/0011-silver-upsert-first-arrival.md`
  - `tests/unit/test_chaos.py`
  - `tests/integration/test_silver.py`
  - `specs/004-silver/{plan,tasks}.md`
- **Changed:**
  - `simulator/{__main__,producer}.py`
  - `tests/unit/{test_producer,test_day}.py` (CLI rename, timestamp)
  - `tests/integration/conftest.py` (chaos produce helper, silver queries, bronze row injection for AC5)
  - `infra/local/docker-compose.yml` (submit the silver jobs, TM memory)
  - `Makefile` (`silver-status`, `silver-rebuild`, demo flag rename)
- **Docs:**
  - `docs/project-spec.md` (silver interfaces, the meaning of `ingest_ts`)
  - `docs/requirements.md` (section 4 keys)
  - `docs/roadmap.md`, `README.md`, `docs/tech-stack.md` if needed
  - `.claude/rules/flink.md`, only if R1 changes the watermark rule
  - the explainer 08:07 chapter status: Dự kiến → Đã chạy

## Order of work
1. Chaos module and CLI (unit, no Docker); `make check` green.
2. Producer record timestamp; spec 001–003 e2e still green.
3. Spike (half a day): Iceberg streaming source with WATERMARK, an upsert sink with a DDL primary key
   into a Trino-created v2 table, and Trino reading the equality deletes. Write the result in plan.md.
4. Silver tables and `silver_gps`; e2e AC2, AC3, AC4, AC9.
5. `silver_orders`; e2e AC5 and AC6.
6. `silver-status` (AC10), restart (AC7), rebuild (AC8), memory (AC11).
7. ADR 0011, docs, `make check`, `make e2e`, `make e2e-restart`; spec Implemented; PR.

## Risks and rollback
- **R1. An Iceberg connector table may not accept WATERMARK, or the upsert sink may not take its key
  from the DDL.** Found in step 3.
  - Watermark fallback: no watermark in silver (nothing in silver uses one), and `flink.md` is updated
    so the watermark moves to feature 6's source.
  - Key fallback: set the Iceberg identifier fields once at init with a small REST call (pyiceberg in the init container).
- **R2. Equality deletes slow down Trino reads.** At dev size this is fine. Compaction is feature 5, and
  the read overhead is noted in ADR 0011.
- **R3. A resubmit without a checkpoint reprocesses all of bronze.** The result is correct (idempotent),
  but slow at the 100M tier. Feature 5 adds resume from the retained checkpoint.
- **R4. Record timestamps in the past and Kafka time-based retention.** This is harmless for 2 h of
  history. It matters for the 30-day backfill, so it is noted for feature 5.
- **R5. Top-1 state for GPS at the standard tier** is about 3.5M keys/day on the heap backend. Dev size is
  about 350k. Feature 5 moves to RocksDB if needed.
- **Rollback:** revert the branch, then drop the `silver` schema in Trino. Bronze and the bronze jobs are
  untouched except for the record-timestamp change, which a revert also undoes.

## Proof
| AC | Test |
|---|---|
| AC1 chaos counts, same distinct pings | unit `test_chaos.py` (exact counts, seeded, order-independent, `send_ts ≥ event_ts`) |
| AC2 clean = chaos in silver | e2e `test_clean_and_chaos_runs_give_identical_silver` (two vehicle prefixes; event fields compared; bronze chaos > clean) |
| AC3 late kept and marked | e2e `test_two_hour_late_ping_is_kept_and_marked_late` |
| AC4 resend changes nothing | e2e `test_resending_pings_changes_no_silver_row` |
| AC5 orders latest by commit position | e2e `test_order_latest_version_wins_despite_duplicates_and_disorder` (older and duplicate rows injected into bronze under a test prefix) |
| AC6 status history | e2e `test_status_history_one_row_per_status_in_commit_order` |
| AC7 hard stop | restart e2e `test_taskmanager_kill_mid_load_keeps_silver_exact` |
| AC8 rebuild | restart e2e `test_silver_rebuild_from_bronze_is_identical` (row hash before and after) |
| AC9 freshness | e2e `test_live_ping_reaches_silver_within_2_min` |
| AC10 status | e2e `test_silver_status_reports_duplicates_and_late_rows` |
| AC11 memory | existing memory e2e with the silver jobs running |

## Verification
- `make check`: unit tests including chaos, no Docker.
- `make clean && make up`, then `make e2e`, then `make e2e-restart`.
- Manual: run `python -m simulator day --chaos`, then `make silver-status`. Bronze has about 10% more
  rows than silver, and the late rows are shown. Then `make stack-mem` stays under 6 GB.

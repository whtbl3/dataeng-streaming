# 0011: Silver built by Flink from bronze: first-arrival Top-1 plus Iceberg upserts
Date: 2026-11-19 · Status: Final (spec 004)

## Context
Bronze keeps every message as it arrived: resent GPS pings, pings flushed after a van was
offline, and every version of every order (spec 004). Silver must hold one row per real event
or entity. It must not change when the same input is processed again: a copy hours later, a
restart, or a full rebuild from bronze (requirements rule 1). It must also be fresh enough for
analysts (about 2 minutes) and stay cheap at the 100M-event tier (ADR 0009).

## Decision
- **Source.** Two pure-SQL Flink jobs read bronze incrementally (Iceberg streaming source,
  `TABLE_SCAN_THEN_INCREMENTAL`), not Kafka. A rebuild is the same job on an empty silver table
  (`make silver-rebuild`).
- **Two layers of dedup.**
  1. *Top-1 in state.* `ROW_NUMBER()` per business key, ordered by data and never by processing
     order:
     - GPS by `(ingest_ts, partition, offset)`, so the first copy wins.
     - Orders by `source_lsn DESC`, so the latest commit wins.
     - Status history by `source_lsn ASC`, so the first commit that reached the status wins.

     State TTL is 24 h for GPS and 7 days for orders.
  2. *Upsert sink.* Each silver table is Iceberg v2 with identifier fields equal to its key, and
     Flink writes with `upsert-enabled`. A copy that arrives after the TTL replaces the same row;
     it can never add a second one.
- **Keys and partitions.** Iceberg upserts need the partition source columns in the key, so:
  - `silver.orders` is keyed by `(order_id, created_at)`;
  - `silver.order_status_history` is keyed by `(order_id, status, status_ts)`.

  Both columns are fixed once set, so uniqueness is unchanged.
- **Who sets the key.** Trino creates the tables, because only it can declare partition
  transforms. Trino cannot set identifier fields, so `infra/local/iceberg/set_identifier_fields.py`
  sets them through the REST catalog API before the silver jobs are submitted. The Flink DDL
  `PRIMARY KEY` is ignored for an existing table.
- **Lateness from data.**
  - `lateness_s = ingest_ts − event_ts`, and `is_late = lateness_s > 30`, the watermark
    allowance of ADR 0006. A flag based on the live watermark would depend on split read order
    and differ between runs.
  - `ingest_ts` is the Kafka record time, which the simulator sets to the device's send time.
  - The GPS source still declares a 30 s watermark on `event_time` (`event_ts` cast to
    millisecond precision, as Flink requires) for later event-time operators (feature 6).
- **Graceful restarts.** `make down` stops every job with a savepoint, and `submit.sh` resumes
  from it. Without HA, a killed JobManager discards its checkpoints. Restarting from Kafka group
  offsets could then rewrite messages already committed to bronze; silver would absorb them, but
  bronze would not be exact.

## Options considered
- **Flink Top-1 + upsert from bronze (chosen):** exact under replay, small state, fresh in about
  70 s. Reads pay for equality deletes until compaction.
- **State-only dedup with append writes:** cheapest reads, but a copy after the TTL (a replay, a
  van offline for a day) creates a duplicate row, and a 100M backfill keeps every key in state.
- **Upsert only (last write wins):** no state, but a late copy rewrites `ingest_ts` and lateness,
  so processing the same input twice changes silver.
- **dbt MERGE every 5 minutes:** simple, but silver is always at least 5 minutes old, and the
  stream has no deduplicated input.
- **Reading Kafka again instead of bronze:** about 1 minute fresher, but it decodes twice and makes
  a rebuild depend on Kafka retention (7 days) instead of bronze (90 days).

## Consequences
- \+ Dirty input gives the same silver rows: clean vs chaos run (AC2), a resend (AC4), a
  taskmanager kill (AC7) and a rebuild (AC8) are all tested.
- − Every upsert writes an equality-delete file next to its data file, so Trino reads slow down
  until compaction. *Update (spec 005, ADR 0012):* the maintenance service compacts silver every
  30 minutes, which applies and drops the delete files; compaction is safe while Flink upserts.
- − A copy arriving after the TTL refreshes `ingest_ts` and `lateness_s` of its row. It is
  counted by `make silver-status`, but the first-arrival guarantee only holds within the TTL.
- − A rebuild re-reads all of bronze. The result is correct, but slow at the 100M tier.
  *Update (spec 005):* state is in RocksDB with incremental checkpoints, so the GPS Top-1 state
  (about 3.5M keys a day at the standard tier) is not capped by the heap.
- − Without HA a killed JobManager does not recover its jobs. *Update (spec 005):* `submit.sh` now
  resumes each job from its newest retained checkpoint when there is no savepoint, so a hard kill
  neither loses nor rewrites bronze messages (tested). Flink HA (ZooKeeper) is still not used: one
  more container (about 300 MB) for a local demo.
- − Silver GPS stays upsert. Spec 005 considered append + dedup, but Flink SQL cannot emit an
  insert-only first-arrival stream (ADR 0012).
- Reverse: drop the `silver` schema and the two jobs; bronze is untouched.

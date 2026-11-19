# Spec 005: Scale (parallelism, cheaper per-event work, table maintenance, bounded storage)
Status: Implemented (2026-11-19) · Intent: roadmap item 5 · Author: project owner + Claude

<!-- WHAT and WHY only. No file names, no code, no SQL. -->

## Problem
The design targets about 100M GPS events (30 days, NFR-11), but several parts cannot grow with it,
and adding machines would not help:
- The GPS topic has 3 partitions, so no more than 3 readers can ever share the work.
- Every event crosses from Flink into Python one row at a time to be decoded (ADR 0010).
- Dedup state lives in memory, so its size is capped by RAM.
- Silver upserts write a delete file with every commit, nothing merges them, and GPS is split into
  16 buckets a day, so files are tiny. Reads get slower with every commit.
- Kafka keeps 7 days by time only, and old snapshots, orphan files and expired partitions are never
  removed (NFR-5 has no proof).

## Outcome
- Work can be split further: more GPS partitions, pings of one van still in order.
- Less work per event: producers batch and compress; the decode handles events in batches.
- State that can grow lives on disk, and checkpoints write only what changed.
- Tables stay healthy on a schedule: right-sized, sorted files; deletes applied; old snapshots,
  orphan files and expired data removed.
- Storage is bounded: Kafka by size as well as time; every table by its retention.
- No large simulated load. Proof uses the existing dev-size tests; the reasoning goes in an ADR.

## Users and scenarios
- **Analyst:** a query for one van on one day reads a few files, not every file of that day.
- **Developer:** leaves the stack running for days; disk and file counts stay flat.
- **Interviewer:** reads one ADR that explains, per layer, what limits scale and what was done.

## Acceptance criteria
- [x] AC1: Given the GPS topic, then it has more partitions than today, all pings of one van land on one partition, and the jobs that read it can run with that many parallel readers.
- [x] AC2: Given producers send batched and compressed, when a simulated day is sent, then bronze holds exactly the same messages as without compression.
- [x] AC3: Given a mix of valid and invalid GPS messages, when they are decoded in batches, then bronze and the dead-letter table hold exactly the same rows and reasons as with the per-row decode (spec 003 tests still pass).
- [x] AC4: Given dedup state on disk with incremental checkpoints, when a job is stopped hard and recovers, then silver is identical to a run without the failure (spec 004 AC7 still passes).
- [x] AC5: Given GPS tables after maintenance, when one van's pings for one day are queried, then fewer files are read than before maintenance (shown by the query statistics), and the result is unchanged.
- [x] AC6: Given a table with small data files and delete files, when maintenance runs, then file and delete-file counts drop, and the rows are identical before and after (checksum).
- [x] AC7: Given silver jobs writing live, when maintenance runs at the same time, then no write fails permanently and silver stays identical to a run without maintenance.
- [x] AC8: Given snapshots older than 7 days, unreferenced files, and partitions older than their retention (NFR-5), when maintenance runs, then they are removed, and current data and running jobs are unaffected.
- [x] AC9: Given a topic at its size bound, when more messages arrive, then the oldest segments are removed and its disk use stays under the bound.
- [x] AC10: Given the developer asks for table health, then they see per table: data files, delete files, average file size, snapshots, and when each maintenance task last ran.
- [x] AC11: Given a table written by parallel writers, when a commit is made, then each partition gets files from one writer, not one file per writer (write distribution by partition).
- [x] AC12: Given a table committed every 30 s for a day, then the number of metadata files and manifests stays bounded (old metadata versions removed, manifests rewritten), not one more per commit forever.
- [x] AC13: Given a streaming job that reads a table incrementally and is stopped, when snapshot expiry runs, then the snapshots that job still needs are kept within the documented limit, and if the job stays stopped longer than that limit, it says so clearly on restart instead of silently skipping data.
- [ ] ~~AC14~~: dropped (2026-11-19). Silver GPS keeps upserts, so a late copy can never add a row (resolved question 6, revised).
- [x] AC15: Given the core and CDC stack with maintenance running, then total container memory stays under 6 GB.

## Out of scope
- Data tiers, `make backfill`, `make bench`, and the `stress` run (removed, see below).
- Autoscaling and more Flink task slots: this feature makes scaling possible, it does not add machines.
- Alert suppression for old events (feature 6); lag and checkpoint dashboards (feature 8).
- Gold retention (no gold yet, feature 7).

## Decisions that change
- **Intent success measure and ADR 0009:** "a 30-day backfill is measured and published" is dropped.
  Tiers, backfill and benchmark become a stretch item. ADR 0009 is marked partly superseded.
- **ADR 0010:** decoding moves from one row at a time to batches (open question 2).
- **ADR 0011:** state backend moves from memory to disk; a killed cluster resumes from retained checkpoints. Silver GPS stays upsert (resolved question 6, revised).
- **ADR 0009 / spec 004:** GPS partitioning `day + bucket(16)` changes (open question 4).
- **NFR-5:** Kafka retention becomes "7 days or a size bound, whichever first".
- New **ADR 0012**: what limits scale per layer, and how tables are maintained. It also records two
  deliberate trade-offs against the usual advice: Python stays in the decode path (batched), and
  bronze GPS stays partitioned by event time rather than arrival time (lateness is at most about 2 h).

## Resolved questions (approved 2026-11-19)
All six recommendations were accepted as written.
1. **How many GPS partitions?** *Recommended:* 6. Room for 6 readers (the laptop has 8 cores) without
   tiny batches. Local jobs keep parallelism 1–2 by config. Existing data is unaffected; only the
   pings sent during the switch may cross partitions, and dedup ranks by data, not order.
2. **How to make the decode cheaper?** Options: (a) *Recommended:* the same Python code receives a
   batch of messages per call instead of one; no new language or build. (b) A JVM function: fastest,
   but adds a Java build to a Python project. (c) Flink's built-in Avro format: fast, but a bad message
   stops the job instead of going to the dead-letter table, which undoes spec 003.
3. **Maintenance schedule?** *Recommended:* locally, a small always-on service through Trino:
   compaction every 30 min for silver and hourly for bronze; expiry, orphan removal and retention
   daily; plus a make target to run it now.
4. **GPS layout?** About 80 MB a day split into 16 buckets gives files of about 5 MB (Iceberg aims
   for 128–512 MB). *Recommended:* partition by day only, with files sorted by vehicle and event time
   during compaction, so a one-van query still skips most files.
5. **Kafka size bound?** *Recommended:* GPS 1 GB per partition (6 GB), CDC 256 MB per partition,
   dead-letter topics 128 MB; time limit stays 7 days.
6. **Silver GPS: upsert or append?** Pings never change and only about 5% are copies, yet upsert
   writes a delete record for every row. The usual advice for event data is append plus dedup.
   *Recommended:* append; the dedup state (24 h) drops copies as today, and maintenance removes the
   rare copy that arrives later (AC14). `silver.orders` keeps upsert, because an order really changes.
   Cost: a job restarted without its state (a rebuild, or a lost checkpoint without HA) would append
   everything again. A rebuild already starts from empty tables; for a lost checkpoint the plan must
   either resume from the last bronze snapshot silver has read, or rely on the maintenance dedup.
   **Revised during the build (2026-11-19, approved by the project owner):** silver GPS stays upsert.
   Flink SQL cannot emit an insert-only stream that keeps the first arrival by data: both a
   data-ordered Top-1 and an event-time deduplicate produce updates (`EXPLAIN CHANGELOG_MODE` shows
   `[I,UA,D]`), which an append sink rejects. The alternatives were a 1-minute window (breaks spec
   004 AC9, silver within 2 minutes) or first-processed-wins (breaks ranking by data, spec 004
   AC8). The spike showed compaction is safe and exact while Flink upserts, so delete files are
   merged every 30 minutes instead of avoided. AC14 is dropped.

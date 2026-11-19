# Spec 005: Scale (data tiers, backfill, table maintenance, bounded retention, benchmark)
Status: Draft · Intent: roadmap item 5 · Author: project owner + Claude

<!-- WHAT and WHY only. No file names, no code, no SQL. -->

## Problem
Today the pipeline is only proven at a few hours of data from 20 vans. Nobody knows how the same
design behaves at the volume it is designed for (ADR 0009, NFR-11): 200 vans × 30 days ≈ 100M
GPS events. Three things already break or will break at that size:
- Silver upserts write a delete file with every commit, and nothing ever merges them. Reads get
  slower with every commit, and small files pile up.
- Kafka keeps 7 days by time only, so a large load can fill the disk; and old snapshots, orphan
  files and expired partitions are never removed (NFR-5 has no proof).
- There is no way to load history through the real path, and no measured number to show: an
  interviewer asks "how fast is it, on what hardware?" and there is no answer.

## Outcome
- Three named data tiers from the same seeded simulator: `dev`, `standard`, `stress` (ADR 0009).
- One command loads N past days through the same Kafka → Flink → Iceberg path as live data.
- Iceberg tables are kept healthy on a schedule: small files and delete files merged, old
  snapshots and orphan files removed, data older than its retention dropped (NFR-5).
- Kafka topics are bounded in size as well as time, so a load cannot fill the disk.
- One command measures a tier and writes a dated result with the hardware it ran on.

## Users and scenarios
- **Developer:** loads 30 days on the laptop, leaves it running, and finds a written benchmark.
- **Analyst:** queries 30 days of silver in seconds, not minutes, after compaction.
- **Interviewer:** reads one result file: events/s per stage, lag, file counts, query times
  before/after compaction, and which stage was the bottleneck, on stated hardware.

## Acceptance criteria
- [ ] AC1: Given a tier name, when a load runs, then the produced volume matches the tier definition and two runs with the same seed produce the same events (unit test on the generator).
- [ ] AC2: Given `DAYS=N`, when a backfill runs, then N past local days of GPS and order events arrive through Kafka and Flink, and bronze row counts equal the produced counts per day (no loss).
- [ ] AC3: Given a backfill that has already run, when the same backfill runs again, then silver is unchanged (same rows and values) and the order database has no duplicate orders.
- [ ] AC4: Given a backfill with chaos on, when it completes, then silver equals the clean backfill of the same days (spec 004 AC2, at backfill size).
- [ ] AC5: Given a table with many small data files and delete files, when maintenance runs, then file count per partition and delete files drop, and the table's rows are identical before and after (checksum).
- [ ] AC6: Given silver jobs writing live, when maintenance runs on silver and bronze at the same time, then no write fails permanently, and silver stays identical to a run without maintenance.
- [ ] AC7: Given snapshots older than 7 days, unreferenced files, and partitions older than their retention (NFR-5), when maintenance runs, then they are removed, and current data and running jobs are unaffected.
- [ ] AC8: Given a topic at its size bound, when more messages arrive, then the oldest segments are removed and disk use stays under the bound; the bound is larger than one full `standard` backfill, so a backfill that keeps up loses nothing.
- [ ] AC9: Given a tier, when the benchmark runs, then a dated result records the hardware, events/s per stage (produce, bronze, silver), peak consumer lag, checkpoint duration, state size, files per partition, disk use, and a fixed set of query times before and after compaction, and names the slowest stage.
- [ ] AC10: Given the `standard` tier on this laptop, when it runs end to end, then it completes, total container memory stays under 8 GB, and the result is committed.
- [ ] AC11: Given the `stress` tier (a backfill plus 2,000 vans live), when it runs, then the live data still reaches silver, and consumer lag returns to near zero after the backfill ends (bounded lag).

## Out of scope
- Alert suppression during catch-up (alert horizon): feature 6 owns alerts.
- A throughput guarantee. The ≥ 20k events/s goal (ADR 0009) is measured, not enforced; if it is
  missed, the result names the bottleneck and the follow-up.
- Replay from chosen offsets and the published `stress` story: feature 11.
- Benchmarks on AWS (feature 10); dashboards for lag and checkpoints (feature 8).
- Gold retention (2 years): there is no gold yet (feature 7).

## Decisions that change
- New ADR 0012: how tables are maintained (who runs compaction, expiry, retention, and when;
  local scheduler vs the AWS managed equivalent).
- ADR 0009 / spec 004: GPS partitioning `day + bucket(16)` may change (open question 3).
- NFR-5: Kafka retention becomes "7 days or a size bound, whichever first" (open question 4).
- ADR 0011: the Flink state backend may move from heap to disk (open question 2).

## Open questions
1. **Where does the backfill's Kafka record time come from?** Since spec 004 the record time is
   the simulated send time, so a 30-day backfill writes records 30 days old. Time-based retention
   could delete them before Flink reads them. *Recommended:* keep the simulated send time
   (lateness stays real and silver equals live), and prove in a spike that retention counts from
   when the broker received the record; if it does not, the GPS topic uses size-only retention.
2. **Dedup state at backfill speed.** Flink's state TTL counts wall-clock time, so 30 days loaded
   in about 90 minutes keeps all ~100M keys in state for 24 h. *Recommended:* move to the disk
   state backend with incremental checkpoints (also what Managed Flink uses), keep the 24 h TTL,
   and record state size in the benchmark. Fallback: a shorter TTL set by config; the upsert sink
   still prevents duplicate rows.
3. **Is `bucket(16, vehicle_id)` right for GPS?** 100M pings are about 2–3 GB of Parquet, about
   80 MB a day. Sixteen buckets give files of about 5 MB, far below the 128–512 MB Iceberg aims
   for. *Recommended:* partition GPS by day only, with files sorted by `(vehicle_id, event_ts)`
   during compaction; the benchmark shows query times for both layouts, and ADR 0009 is updated
   with the measured reason.
4. **Kafka size bound.** *Recommended:* GPS topic bounded per partition so the topic holds a bit
   more than one full `standard` backfill (about 12 GB total), CDC topics smaller; time limit stays
   7 days. The benchmark records peak topic size.
5. **Who runs maintenance, and how often?** *Recommended:* locally, a small always-on service
   that runs through Trino: compaction every 30 min for silver and hourly for bronze, expiry and
   orphan removal daily, retention daily; plus a make target to run it now. On AWS, the Glue
   table optimizers do the same by config (ADR 0005). ADR 0012 records it.
6. **Is `stress` in this feature?** *Recommended:* yes, as a runnable tier and one recorded run of
   about 30 minutes; the long run and its write-up stay in feature 11.
7. **Result file name.** ADR 0009 says `<date>-<tier>-<host>`. *Recommended:* `<host>` is a short
   hardware label (for example `8c-10g-wsl2`), never the machine's hostname, so nothing personal
   is committed.

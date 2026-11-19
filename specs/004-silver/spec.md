# Spec 004: Silver (dedup, event-time watermarks, chaos mode)
Status: Implemented (2026-11-19) · Intent: roadmap item 4 · Author: project owner + Claude

<!-- WHAT and WHY only. No file names, no code, no SQL. -->

## Problem
Bronze stores every message exactly as it arrived, and that is its job. Real GPS devices resend
pings they are not sure were delivered, lose signal in tunnels and flush old pings later, and
reach Kafka out of order. Bronze therefore contains duplicates and late events, and the order
changelog holds many versions of each order. Nothing downstream can use it directly: counting
bronze rows double-counts distance, "the last row" moves a van backwards, and "the current
state of an order" needs the whole changelog. Today the simulator sends only clean data, so
none of this is tested.

## Outcome
- Silver holds one row per real event or entity, by business key (requirements, section 4):
  - `silver.gps_pings`: one row per ping `(vehicle_id, event_ts)`;
  - `silver.orders`: one row per order, the version with the highest commit position;
  - `silver.order_status_history`: one row per status an order reached, with its time.
- Late events are kept, not dropped; each silver ping says whether it was late and by how much.
- Processing the same input twice (duplicates hours later, a restart, a rebuild) changes nothing.
- The simulator can send dirty data on purpose, in known amounts, so tests can prove the above.

## Users and scenarios
- **Analyst:** counts pings or distance per van from silver without thinking about duplicates.
- **Feature 6 and 7 (internal users):** read deduplicated, event-time-ordered data; gold KPIs
  are built from silver, so NFR-2 (identical KPIs on dirty data) starts here.
- **Interviewer:** sees the same day sent clean and dirty, and silver comes out identical.

## Acceptance criteria
- [x] AC1: Given a seed and chaos settings (duplicate share, delayed share with a maximum delay up to 2 h, out-of-order jitter, offline bursts), when a day is simulated, then the exact number of events of each kind is known, and the set of distinct pings equals the clean run with the same seed (unit test).
- [x] AC2: Given the same simulated day sent once clean and once with chaos, when both reach silver, then silver GPS has identical rows for both (same keys and values), while bronze for the chaos run has more rows.
- [x] AC3: Given a ping that arrives after the watermark has passed its event time (for example 2 h late), when it is processed, then it is in silver, marked late, with its lateness (arrival time minus event time) stored.
- [x] AC4: Given pings already in silver, when the same pings arrive again hours later, then silver gets no new or changed rows.
- [x] AC5: Given N changes to an order, including duplicates and changes that reach the job out of commit order, when they are processed, then silver holds exactly one row for that order, equal to the change with the highest commit position.
- [x] AC6: Given a delivered order, then its status history has one row each for created, assigned, out for delivery and delivered, with the time of each change, ordered by commit position; a failed order ends with failed.
- [x] AC7: Given a silver job is stopped hard in the middle of a load, when it recovers, then silver is identical to a run without the failure (no loss, no duplicates).
- [x] AC8: Given silver is dropped, when it is rebuilt from bronze, then it is identical to the silver built live.
- [x] AC9: Given live mode, when a ping is produced, then it is queryable in silver within 2 minutes.
- [x] AC10: Given the developer asks for silver status, then they see, per table and day, bronze rows, silver rows, duplicates removed and late rows.
- [x] AC11: Given the core and CDC components plus the silver jobs run with the dev-size simulation, then total container memory stays under 6 GB.

## Out of scope
- Gold KPIs and the "dirty input ⇒ identical KPIs" test on gold (feature 7). This feature proves it on silver.
- Compaction, snapshot expiry and silver retention (feature 5).
- Read restriction on `dest_lat` / `dest_lon` (NFR-6): no access control in the local stack; gold excludes them (feature 7).
- Checking that order statuses move in a valid order (dbt tests, feature 7).
- Silver tables for depots and vans: `dim_depot` and `dim_van` (SCD2) are built by dbt from bronze (feature 7).
- The live join, ETA and alerts (feature 6).

## Decisions that change
- New ADR 0011 for how silver is written (resolved questions 1 and 2).
- Simulator option `--late-rate` (late *deliveries*, spec 002 AC4) is renamed so it is not confused with late *events* (resolved question 5).

## Resolved questions (approved 2026-11-19)
All six recommendations were accepted as written.
1. **Where do silver jobs read from?** *Recommended:* from bronze (Iceberg, read incrementally), not from Kafka again. A rebuild (AC8) is then the same job started from the first bronze snapshot, the decode and DLQ stay in one place, and bronze stays the only source of truth (rule 1). Cost: silver is about 1 minute behind bronze (two 30 s commits), which is why AC9 says 2 minutes. The alert path (feature 6) does not wait for silver.
2. **How does silver GPS stay unique?** Options:
   - (a) Flink remembers seen keys for a time (state TTL) and appends only new ones. Cheap to read, but a duplicate that comes after the TTL (a replay, a van offline for a day) gets in, and a 100M-event backfill holds every key in state.
   - (b) *Recommended:* the silver table has a primary key and Flink writes upserts. Any duplicate, at any time, replaces the same row, so AC4, AC7 and AC8 hold without large state. Cost: each write also writes a delete marker, so reads slow down until compaction (feature 5 measures it).
   - (c) dbt MERGE every 5 min. Simple, but silver is then 5 min stale and the stream has no deduplicated input.
   `silver.orders` is upserted by `order_id`, and an older commit position never overwrites a newer one. Status history is keyed by `(order_id, status)`.
3. **What counts as late?** *Recommended:* a watermark 30 s behind the highest event time seen (ADR 0006). A ping behind the watermark is marked late but still written; lateness = Kafka arrival time minus event time.
4. **Chaos defaults?** *Recommended:* off by default; one `--chaos` switch sets 5% duplicates (1–3 copies, seconds apart), 5% delayed (mostly under 10 min, a few up to 2 h), out-of-order jitter up to 10 s, and one offline burst of 2–15 min per van per day whose pings are flushed when the van reconnects. Each share can be set on its own.
5. **Rename `--late-rate`?** *Recommended:* yes, to `--late-delivery-rate`. The chaos options use "delay" for late events.
6. **Two copies with the same key but different values?** *Recommended:* the simulator never makes them (a duplicate is a resend). If they occur, the last one written wins and silver status counts them; this is the only place arrival order decides, and only between copies of one event.

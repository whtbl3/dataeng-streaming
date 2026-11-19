# Spec 007: Gold (delivery KPIs with dbt, intraday and nightly close)
Status: Approved (2026-11-19) · Intent: roadmap item 7 · Author: project owner + Claude

<!-- WHAT and WHY only. No file names, no code, no SQL. -->

## Problem
Managers still get no KPIs: on-time rate, ETA error, depot performance and alert quality exist
nowhere. The intent's key promise is unproven: late or duplicate events must give the same KPIs
as clean data (NFR-2), intraday and after the nightly close (ADR 0006). Two inputs are also
missing from the lakehouse: alerts and past ETAs live only in Postgres serving, which keeps the
latest ETA and nothing about what was predicted 30 or 15 minutes before the deadline.

## Outcome
- A gold star schema built by dbt from silver and bronze (requirements, section 4): one fact row
  per closed order, a daily aggregate per depot, van versions over time, depots and dates.
- Every 5 minutes the open days are recomputed over a 2 h look-back and marked provisional; at
  01:00 local the previous day is recomputed in full, marked final and recorded as closed.
- The KPIs a manager asks for: on-time rate and failure rate (BR-2), minutes late, ETA error at
  T-30 and T-15 (resolved question 2 of the requirements), stops per hour, alert lead time,
  precision and recall (BR-5), per depot and local day (BR-4).
- Data-quality tests block a bad model, locally and in CI.
- The same day sent clean and with chaos gives identical gold KPIs.

## Users and scenarios
- **Operations manager:** "yesterday, depot D1: on-time 91 %, ETA error at T-15 was 3 min".
- **Analyst:** queries gold with SQL and can tell a provisional number from a final one.
- **Interviewer:** sees a chaos day and a clean day give the same KPIs, and a late event after
  the close being counted, not silently applied.

## Acceptance criteria
- [ ] AC1: Given alerts and ETAs from the control tower, then every alert raise/resolve and the ETA of each open order at T-30 and T-15 before its promised end are stored in the lakehouse, append-only and replay-safe.
- [ ] AC2: Given closed orders in silver, when gold is built, then there is exactly one fact row per closed order with its local day (Asia/Ho_Chi_Minh), outcome, on-time flag (a failed order is not on time), minutes late, ETA error at T-30 and T-15, first alert time and alert lead time, and the van version that was valid when it closed.
- [ ] AC3: Given gold, then no table holds a customer's exact destination; only a coarse area (about 150 m) is kept (NFR-6).
- [ ] AC4: Given a van that moves to another depot, when gold is built, then its earlier deliveries stay with the old depot and later ones go to the new one (versioned van dimension).
- [ ] AC5: Given orders around local midnight, then each lands on the right local day (BR-4), in the fact and in the daily aggregate.
- [ ] AC6: Given the daily aggregate per depot and day, then on-time rate, failure rate, stops per hour, mean and p90 ETA error, alert precision and recall equal the same figures computed by hand from the fact rows (data test).
- [ ] AC7: Given the intraday run every 5 minutes, then today's and yesterday's provisional rows reflect every change within the last 2 h of event time, are marked provisional, and a run touches only that window.
- [ ] AC8: Given the nightly close for a local day, then that day is recomputed in full, marked final, and recorded as closed with when it closed and how many late events it absorbed; running it again gives the same result.
- [ ] AC9: Given an event for a closed day arrives after the close, then the closed day does not change, the arrival is counted as late-after-close, and an explicit, logged restatement of that day applies it.
- [ ] AC10: Given the same simulated day sent clean and with chaos (spec 004), then gold KPIs are identical after the intraday run (for events up to 2 h late) and after the close (NFR-2).
- [ ] AC11: Given a model change that breaks a data test (uniqueness, not-null, accepted values, relationships, the hand-computed KPIs), then the build fails locally and in CI.
- [ ] AC12: Given silver stops receiving data, then a source freshness check warns after 10 min and fails after 30 min.
- [ ] AC13: Given the core and CDC stack with all jobs and the gold scheduler running at dev size, then total container memory stays under 6 GB.

## Out of scope
- Dashboards (feature 8); Athena and dbt-athena (feature 9).
- ML ETA, route quality, driver scoring.
- Restating more than one day at a time; backfilling gold for history older than silver.

## Decisions that change
- The control tower also writes alerts and ETA marks to the lakehouse (spec 006 adds an output).
- New ADR 0014: how gold is built and scheduled (dbt on Trino, incremental windows, nightly close,
  where the connection settings live).

## Resolved questions (approved 2026-11-19)
All seven recommendations were accepted as written.
1. **How do alerts and past ETAs reach the lakehouse?** *Recommended:* the control tower job also
   appends them to two bronze Iceberg tables (exactly-once, in the same commit as its Kafka output):
   every alert event, and an ETA mark per open order when it passes T-30 and T-15. Reading
   Postgres serving from Trino instead would make gold depend on a table that only keeps the
   latest ETA and is not the source of truth (rule 1).
2. **Where do the dbt connection settings live?** Secret files must not be read or edited, and
   `profiles.yml` is one. *Recommended:* commit a `profiles.yml.example` that takes every value from
   environment variables (no secrets in it); the dbt container and the make targets copy it to a
   temporary profiles directory. Passwords come from the same env defaults as the rest of the stack.
3. **Scheduler?** *Recommended:* a small `dbt` container running a loop: the intraday build every
   5 min and the nightly close at 01:00 local (ADR 0006; no Airflow, tech stack). `make gold`,
   `make close DAY=…` and `make reclose DAY=…` run them by hand.
4. **Fact table write mode?** *Recommended:* incremental MERGE on `order_id` over orders whose events
   fall in the last 2 h (intraday); the close overwrites the whole local-day partition. Small
   dimensions are rebuilt in full every run.
5. **Data tests in CI?** CI has no stack. *Recommended:* CI compiles the project, lints the SQL and runs
   dbt unit tests (fixed input rows → expected KPIs) on a throwaway Trino container with an in-memory
   catalog; the full data tests on real tables run in the e2e suite.
6. **ETA error definition?** *Recommended:* ETA mark at T-30 (and T-15) minus the actual arrival, in
   minutes, signed (positive = predicted too late), reported as mean absolute and p90; orders without
   a mark (dispatched less than 30 / 15 min before their deadline) are left out and counted.
7. **Stops per hour?** *Recommended:* closed orders of a van ÷ hours between its first dispatch and its
   last close that day, averaged per depot.

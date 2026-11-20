# 0014: Gold with dbt on Trino: a commit-time intraday window and a nightly close
Date: 2026-11-19 · Status: Final (spec 007)

## Context
Managers need delivery KPIs per depot and day (on-time rate, minutes late, ETA error, stops per
hour, alert quality). ADR 0006 decided the shape: provisional numbers every 5 minutes over a 2 h
look-back, and a nightly close that makes a day final. Late and duplicate events must not change
the KPIs (NFR-2), and a closed day changes only by an explicit restatement (requirements, rule 4).

## Decision
- **dbt-core + dbt-trino** build a star schema in `lakehouse.gold`:
  - `fct_delivery`: one row per closed order;
  - `agg_depot_daily`: depot × local day;
  - `dim_van`: SCD Type 2 from the vans changelog;
  - `dim_depot`, `dim_date`;
  - `closed_days`.

  Staging views live in `gold_staging`. All days are Asia/Ho_Chi_Minh (BR-4). The destination is
  kept as a Bing tile quadkey at zoom 18, about 150 m (NFR-6); exact coordinates stay in silver.
- **Intraday (every 5 min):** `fct_delivery` MERGEs on `order_id` the orders whose silver row,
  alert or ETA mark was *committed* in the last 2 h. Using system time (rule 3) rather than event
  time means a late change of any age is absorbed within 5 minutes. Closed days are never touched.
  `agg_depot_daily` rewrites the open days from the facts. Rows are `is_final = false`.
- **Close (after 01:00 local):** for each past day that is not closed, oldest first, overwrite its
  partition (`delete+insert` on `local_date`), set `is_final = true` and record it in
  `closed_days` (`closed_at`, late events absorbed). A second close of the same day changes
  nothing. Every intraday run also counts changes committed after a close (`late_after_close`).
  `make reclose DAY=…` restates a day and adds one to its `restatements`.
- **Alerts and ETA history** come from bronze tables that the control tower appends
  (`bronze.alert_events`, `bronze.eta_marks`, exactly-once), not from Postgres serving, which keeps
  only the latest ETA.
- **Scheduler:** a stdlib Python loop in a small `dbt` container runs the builds, with no
  orchestrator. Each build runs the data tests too, so a failing test fails the build.
- **Tests:**
  - schema tests on every key;
  - singular tests: the aggregate equals the facts, no exact destination, closed days are final;
  - dbt unit tests with fixed rows for BR-2, BR-4, SCD2, ETA error and stops per hour.

  CI runs the unit tests and sqlfluff against a throwaway Trino with a memory catalog (`make
  dbt-ci`), so CI needs no stack.
- **Connection settings:** a committed `profiles.yml.example` takes every value from the
  environment and is copied to a temporary profiles directory at run time; no `profiles.yml`
  lives in the repo.

## Options considered
- **dbt on Trino, scheduler loop (chosen):** SQL models with tests and docs; the same models run on
  Athena (dbt-athena, feature 9); two schedules do not need Airflow.
- **Intraday window by event time (2 h of `closed_at`):** matches ADR 0006 literally, but a change
  arriving later than 2 h waits for the close; the commit-time window absorbs it at once.
- **Flink SQL for gold:** fresher, but KPIs over a closed day and restatements are batch problems,
  and gold would depend on stream state.
- **Airflow or Dagster:** better observability and backfills, but one more heavy service for two
  schedules (tech stack).

## Consequences
- \+ The clean and the dirty run of a day give identical delivery KPIs, both provisional and final
  (spec 007 AC10).
- \+ A late change is visible within 5 minutes on open days and is counted on closed days.
- − Alert precision and recall and ETA error come from what the stream saw. Under GPS chaos they can
  differ on borderline cases (spec 006), so they are reported but are not part of the clean = dirty
  guarantee.
- − ETA error uses the close time as the arrival (the order system records no separate arrival),
  so it includes the dwell at the stop (2-6 min in the simulator).
- − Each intraday run writes a snapshot of `fct_delivery`; the maintenance service compacts gold
  like the other layers.
- Reverse: stop the `dbt` service and `DROP SCHEMA lakehouse.gold CASCADE`; silver is untouched.

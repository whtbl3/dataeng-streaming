# Plan 007: Gold (delivery KPIs with dbt, intraday and nightly close)
From: spec.md (Approved 2026-11-19) · Approved by: project owner (2026-11-19)
## Context
Spec `specs/007-gold/spec.md` (Approved 2026-11-19, 13 ACs, 7 resolved questions). Managers get
no KPIs yet, and the intent's key promise (dirty input → identical KPIs, NFR-2, ADR 0006) is
unproven on gold. Alerts and past ETAs live only in Postgres serving. This feature adds the alert
and ETA-mark history to bronze, a dbt project on Trino building the gold star schema, a scheduler
for the 5-min intraday run and the 01:00 close, and data tests locally and in CI.
Branch `feat/007-gold` is stacked on `feat/006-control-tower` (PR #15).

## Refinements to approve with this plan (from reading the code)
1. **The intraday window is by commit time, not event time.** Orders whose silver row changed in
   the last 2 h (`last_commit_ts`, system time, rule 3) are recomputed. A late change of any age is
   then absorbed within 5 min instead of waiting for the close; closed days are never touched.
2. **AC10 compares the delivery KPIs** (outcome, on-time, minutes late, failure rate, stops per
   hour), which come from orders. Alert and ETA metrics describe what the stream saw; spec 006
   showed they can differ on borderline cases under GPS chaos, so they are excluded from the
   equality and reported. The test's dirty run injects duplicate and out-of-order CDC rows into
   bronze (as spec 004 AC5) and GPS chaos.
3. **Destination area = Bing tile quadkey at zoom 18 (≈150 m)**, a built-in Trino function, instead
   of a 7-character geohash (Trino has no geohash function). Same precision; requirements updated.
4. **ETA marks:** a mark is written when the first ETA at or after T-30 (T-15) is computed and the
   order already had an ETA before that time; an order dispatched later gets no mark (spec Q6).

## Spike outcome and departures (build, 2026-11-19)
- **S1 dbt-trino on Iceberg REST works:** `merge` (intraday) and `delete+insert` on `local_date`
  (close) on partitioned Iceberg tables; views in `gold_staging`; Bing tiles and `at_timezone` in
  Trino. Found: `current_timestamp` is precision 3 and Iceberg wants 6 (cast); a `ref` inside a
  Jinja branch needs a `-- depends_on:` hint; dbt-trino's default incremental strategy is
  `append`, so `closed_days` sets `merge` (a restatement first added a second row).
- **S2 CI on a memory catalog works** with empty source tables (`infra/local/dbt/ci_sources.sql`)
  and `dbt run --empty`. Two changes: the CI target gets no `partitioning` property (the memory
  connector has none; `partitioned_by()` macro), and expected rows use the dict format (a SQL
  fixture for `expect` must list every model column).
- **S3 the PyFlink job writes Iceberg** in the same statement set as JDBC and Kafka. The `tower`
  operator now has a fixed uid, so its per-van state restores after the job graph changes; this
  deployment started the control tower fresh (`FRESH_JOBS`) to re-read the order changelog.
- **sqlfluff `fix` dropped the `unnest(...) as t (d)` alias** in `dim_date` (rule AL05); restored
  with `-- noqa: AL05`, and the whole auto-fix diff was reviewed (formatting only otherwise).
- **Gold is in the maintenance service** (compaction with silver every 30 min; retention 2 years,
  NFR-5); the scheduler gained `--test` and `--freshness` for `make gold-test` and the AC12 test.
- **Connection settings:** the dbt rules say `profiles.yml` is committed with `env_var()`, while the
  project rules say never to read or edit `profiles.yml`. The approved spec keeps
  `profiles.yml.example` (env vars only, copied at run time); the owner decides which rule wins.
- **ETA error uses the close time as the arrival** (the order data has no arrival time); noted in
  ADR 0014.
- `minutes_late` is 0 for an on-time delivery and NULL for a failure.
- A `.PHONY` typo from spec 006 (`table-healthaws-plan`) was fixed here and on `feat/006-control-tower`.

## Design
**Control tower → bronze** (`flink/udf/tower.py`, `flink/jobs/control_tower.py`):
- New output `EtaMark(order_id, van_id, mark 'T30'|'T15', eta, promised_end, event_ts)`; state per
  order keeps its previous ETA time and marks already written.
- The job gets an Iceberg catalog (same env as SQL jobs) and two append sinks in its statement set:
  `bronze.alert_events` (alert_id, event, type, van_id, order_id, raised_at, resolved_at, lat, lon,
  detail, event_ts) and `bronze.eta_marks`. Tables in `infra/local/trino/init/006_tower.sql`,
  partitioned by `day(event_ts)`; exactly-once (Iceberg commits with the checkpoint).

**dbt project** (`dbt/`, dbt-core 1.11.16 + dbt-trino 1.11.0, sqlfluff 4.4.0 jinja templater):
- `profiles.yml.example` (all `env_var`, no secrets); container and make targets copy it to
  `/tmp/dbt-profiles` (resolved question 2). Target schema `gold`.
- `models/staging/`: `stg_orders_closed` (silver.orders delivered/failed, `local_date` in
  Asia/Ho_Chi_Minh, `dest_area`, arrival from status history), `stg_van_versions` (bronze.vans_cdc by
  `source_lsn` → valid_from/valid_to), `stg_depots` (latest), `stg_alert_events`, `stg_eta_marks`
  (deduplicated on (alert_id, event) / (order_id, mark)).
- `models/marts/`:
  - `dim_date`, `dim_depot`, `dim_van` (SCD2, `van_key` surrogate): table, rebuilt each run;
  - `fct_delivery`: incremental, `merge` on `order_id`, partitioned by `local_date`, `is_final`.
    Intraday (`var mode=intraday`): orders with `last_commit_ts >= now - 2h` (or arrivals/alerts/marks
    newer), excluding closed days. Close (`mode=close`, `day=D`): `delete+insert` of partition D,
    `is_final = true`;
  - `agg_depot_daily`: incremental `delete+insert` of the local days the run touched;
  - `closed_days`: one row per closed day (closed_at, late_events_absorbed = changes committed
    after D ended and before the close, late_after_close updated by every intraday run,
    restatements count).
- Tests: schema tests (unique, not_null, accepted_values, relationships); singular tests
  `agg_matches_fact` (AC6), `gold_has_no_exact_destination` (AC3, information_schema),
  `closed_days_are_final`; dbt unit tests for on-time/failed (BR-2), local-day boundaries (BR-4),
  SCD2 lookup (AC4), ETA error and stops per hour definitions (spec Q6, Q7).
- Source freshness on silver.orders and silver.gps_pings: warn 10 min, error 30 min (AC12).

**Scheduler** (`dbt/scheduler.py`, stdlib + subprocess, in a `dbt` compose service built from
`infra/local/dbt/Dockerfile`, python:3.11.11-slim + pinned dbt): every 5 min `dbt build --vars
mode: intraday`; after 01:00 local, close every past day with data that is not closed yet (also
catches up after downtime). Make: `make gold`, `make close DAY=…`, `make reclose DAY=…` (logged
restatement), `make gold-test`, `make gold-status`.

**CI** (`.github/workflows/ci.yml`, `check` job): service container `trinodb/trino:484` with a
memory catalog named `lakehouse`; `make dbt-ci` creates the source tables empty (a setup SQL),
`dbt run --empty`, `dbt test --select test_type:unit`, `sqlfluff lint dbt/`. `make check` stays
offline; `make dbt-ci` is the extra CI step (and runs locally with the stack's Trino).

## Files that change
- **New:** `dbt/{dbt_project.yml,profiles.yml.example,scheduler.py}`, `dbt/models/**`,
  `dbt/tests/**`, `dbt/macros/**`, `dbt/.sqlfluff`, `infra/local/dbt/{Dockerfile,ci_sources.sql}`,
  `infra/local/trino/init/006_tower.sql`, `docs/decisions/0014-gold-dbt-intraday-and-close.md`,
  `tests/unit/test_scheduler.py`, `tests/integration/test_gold.py`, `specs/007-gold/{plan,tasks}.md`.
- **Changed:** `flink/udf/tower.py` (+ `tests/unit/test_tower.py`), `flink/jobs/control_tower.py`,
  `infra/local/docker-compose.yml` (dbt service), `Makefile`, `pyproject.toml` (dependency group
  `dbt`), `.github/workflows/ci.yml`, `tests/integration/conftest.py`.
- **Docs:** ADR 0014; `docs/requirements.md` (dest_area, intraday window), `docs/project-spec.md`,
  `docs/roadmap.md`, `README.md`, data-flow and gold ERD diagrams (built).

## Order of work
1. **Spikes:** S1 dbt-trino on Iceberg REST: incremental `merge` and `delete+insert` with
   `local_date` partitions, `on_table_exists`, timestamp-with-tz types. S2 dbt unit tests on a
   Trino memory catalog with empty source tables. S3 PyFlink Iceberg sink in the control tower job.
2. Tower ETA marks + bronze sinks (AC1).
3. dbt project skeleton, profiles example, staging, dimensions (AC3, AC4, AC5).
4. `fct_delivery`, `agg_depot_daily`, `closed_days`, intraday and close modes (AC2, AC6-AC9).
5. Tests: schema, singular, unit; freshness (AC11, AC12); CI step.
6. Scheduler service + make targets; e2e AC10 clean vs dirty; memory AC13.
7. ADR 0014, docs; `make check`, `make e2e`, `make e2e-restart`; spec Implemented; PR.

## Risks and rollback
- **R1 dbt-trino merge on Iceberg** with partition evolution or tz types (S1). Fallback:
  `delete+insert` everywhere (Trino Iceberg row-level delete is supported).
- **R2 CI unit tests need input relations** for type inference (S2). Fallback: fixtures in
  `format: sql` with every column, or run unit tests only in e2e.
- **R3 Trino load**: dbt every 5 min plus maintenance. Keep intraday small (2 h window); stagger
  with maintenance; Trino stays at 1.5 GB.
- **R4 Memory** (AC13): dbt container idles at ~50 MB, peaks ~300 MB during a run.
- **Rollback:** revert the branch; `DROP SCHEMA lakehouse.gold CASCADE`; drop the two bronze tables;
  stop the `dbt` service. Silver and the jobs are unaffected (the tower sinks are additive).

## Proof
| AC | Test |
|---|---|
| AC1 alert/ETA history | unit `test_tower.py` (marks); e2e `test_alert_events_and_eta_marks_reach_bronze` |
| AC2 one fact per closed order | dbt unique/not_null + e2e row count vs silver |
| AC3 no exact destination | singular dbt test on information_schema |
| AC4 van moves depot | dbt unit test (SCD2) + e2e (update a van's depot) |
| AC5 local midnight | dbt unit test (orders at 23:59 / 00:01 local) |
| AC6 aggregate = hand-computed | singular dbt test `agg_matches_fact` |
| AC7 intraday window | e2e: change an order, `make gold`, only its day changes, `is_final = false` |
| AC8 close | e2e `make close DAY=…` twice: same rows, `is_final`, `closed_days` row |
| AC9 late after close | e2e: change a closed day's order → unchanged, counted; `make reclose` applies, logged |
| AC10 clean = dirty | e2e two prefixes, same seed; dirty gets CDC dup/out-of-order + GPS chaos |
| AC11 tests block | CI `make dbt-ci`; e2e: a broken model fails `make gold-test` |
| AC12 freshness | e2e `dbt source freshness` with a stale fixture source |
| AC13 memory | existing memory e2e with the dbt service, < 6 GB |

## Verification
- `make check`; `make dbt-ci` (unit tests + lint on a throwaway Trino).
- `make up`, `make demo` for a while, `make gold`, `make gold-status`; `make close DAY=yesterday`;
  query `gold.agg_depot_daily` in Trino.
- `make e2e`, `make e2e-restart`, `make stack-mem` < 6 GB.

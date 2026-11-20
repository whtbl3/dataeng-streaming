# Plan 008: Dashboard design and pipeline health checks
From: spec.md (Approved 2026-11-20) · Approved by: project owner (2026-11-20)
## Context
Spec `specs/008-dashboards/spec.md` (Approved 2026-11-20, 4 ACs). The owner chose not to build or
deeply test real dashboards: a precise dashboard design plus one `make health` command for the
four NFR-10 conditions. Light tests only.

## Design
**Dashboard design** (`docs/design/dashboards.md` + `docs/design/dashboards.sql`):
- Three dashboards, a table per dashboard: panel, question it answers, source (Postgres serving,
  Trino gold, metrics), refresh, visual, and the provisional marker (KPI rows with
  `is_final = false` shown with a "provisional" badge; a final-only variable).
  - Dispatch (5 s): van map (`serving.vehicle_latest`), open alerts, at-risk orders (ETA past
    promised end), alerts per hour.
  - KPI (5 min): on-time rate, failure rate, ETA MAE/p90, stops per hour, alert precision/recall per
    depot and day (`gold.agg_depot_daily`), late after close (`gold.closed_days`).
  - Pipeline health (30 s): consumer lag, records/s and checkpoints per job (Flink and Redpanda
    metric names to use), slot lag, newest row per layer, maintenance runs (`ops.maintenance_runs`).
  - The NFR-10 alert rules as the tool should define them (condition, window: e.g. lag grows for
    5 min).
- Every SQL panel query is in `dashboards.sql`, one block per panel:
  `-- panel: <dashboard>.<panel> (postgres|trino)` then the query. The doc references them by name.

**Health check** (`scripts/health.py`, run with `uv run`; `make health`):
- Pure rule functions (unit tested): `lag_alert(first, second, minimum=2000)`,
  `checkpoint_alert(latest_failed_ts, latest_completed_ts)`, `slot_alert(bytes, 500 MB)`,
  `bronze_stuck(newest_kafka_ts, newest_bronze_ts, now, 2 min)`.
- Collectors: Kafka lag per consumer group = high watermark − committed offset (confluent-kafka
  AdminClient, two samples 30 s apart); Flink REST `/jobs/overview` + `/jobs/<id>/checkpoints`;
  Postgres `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)` for `ct_cdc_slot`; newest GPS record
  time per partition (last message) vs `max(ingest_ts)` of `bronze.gps_pings` (Trino).
- Output: one line per check, `OK` or `ALERT` with the value; exit 1 if any ALERT.
  `--quick` skips the 30 s wait (one lag sample, lag growth not judged) for tests.

## Files that change
- **New:** `docs/design/dashboards.md`, `docs/design/dashboards.sql`, `scripts/health.py`,
  `tests/unit/test_health.py`, `tests/integration/test_health.py`, `specs/008-dashboards/{plan,tasks}.md`.
- **Changed:** `Makefile` (`health`), `README.md`, `docs/project-spec.md` (CLI, interfaces),
  `docs/requirements.md` (NFR-10 proof), `docs/intent.md` (desired outcome changed),
  `docs/decisions/0007-grafana-postgres-serving.md` (deferred note), `docs/roadmap.md`
  (Implemented; Grafana dashboards in stretch).

## Order of work
1. Unit tests, then `scripts/health.py` rules and collectors; `make health` (AC3, AC4).
2. `dashboards.sql` + `dashboards.md` (AC1); light e2e running every query and `make health` (AC2, AC4).
3. Docs; `make check`; spec Implemented; PR.

## Risks and rollback
- **Flink commits Kafka offsets only on checkpoints:** lag rises and falls every 30 s, so the lag
  rule needs both growth and a minimum (2,000 messages ≈ 50 s of live GPS at dev size).
- Rollback: revert; nothing in the running stack changes.

## Proof
| AC | Test |
|---|---|
| AC1 design | review of `dashboards.md` (every panel has question, source, query name, refresh, marker) |
| AC2 queries run | e2e `test_every_dashboard_query_runs` |
| AC3 checks | unit `test_health.py` (each rule); e2e `test_health_reports_every_check` |
| AC4 exit code | unit (any ALERT → 1); e2e healthy stack → 0 |

## Verification
- `make check`; `make up`, `make health` (all OK), stop `bronze_gps` while `make demo` runs →
  `make health` shows bronze stuck ALERT and exits 1; `uv run pytest tests/integration/test_health.py`.

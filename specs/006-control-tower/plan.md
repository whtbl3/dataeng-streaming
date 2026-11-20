# Plan 006: Control tower (live ETA, SLA-risk and abnormal-stop alerts)
From: spec.md (Approved 2026-11-19) · Approved by: project owner (2026-11-19)
## Context
Spec `specs/006-control-tower/spec.md` (Approved 2026-11-19, 12 ACs, 7 resolved questions). This is
the feature the intent is about: a live ETA per open order and an alert before an SLA breach, plus
abnormal-stop alerts, written to Postgres `serving` (ADR 0007) and `alerts.v1`, GPS → alert p95 < 10 s.
On approval this becomes `specs/006-control-tower/plan.md` + `tasks.md`.

## Refinements to approve with this plan (from reading the code)
1. **One pure state machine, two harnesses.** All rules (latest position, ETA, SLA risk, abnormal
   stop, horizon) live in pure Python (`flink/udf/tower.py`, per the Flink rules). Flink
   wraps it in a keyed process function; tests also replay a whole simulated day through it
   offline. AC5 (precision/recall) and AC8 (clean = chaos) are proven by that offline replay: a full
   day in seconds, deterministic, no wall clock. The e2e tests prove the wiring with live-time events.
2. **No event-time timers or global watermark.** Pings keep coming every 5 s while a van stands
   still, so "idle ≥ 10 min" is decided on the van's own pings. With 6 partitions and few live vans,
   a global watermark would stall on idle partitions and timers would never fire. A van that sends
   nothing is offline, not stopped, and raises no abnormal-stop alert.
3. **AC8 is checked on the set of alerts** (type + order or van), and on raise times equal to the
   clean run except where chaos delayed the ping that triggers the alert (then later by at most that
   delay). A ping older than the van's latest position is ignored (AC3), so a jittered trigger ping
   moves the raise to the next ping; an offline burst older than the horizon cannot raise.
4. **The alert horizon uses processing time** (now − `event_ts` > 10 min → update state, no alert),
   with the clock injected, so the offline replay uses event time as "now".
5. **Abnormal stops are injected on the return hop to the depot** (after the last delivery). The
   orders and every other ping stay exactly as in the clean day (AC1 "otherwise unchanged").
6. **Depot location** comes from a cached lookup join against Postgres (`vans` ⋈ `depots`, a static
   dimension); the order events' event time is `updated_at` (business time).

## Spike outcome (T1, 2026-11-19)
- **S1 alert quality (offline replay, 3 days × 20 vans × 500 orders, seeds 7/11/23).**
  - As approved (recent speed on every leg): recall 61-69 %, precision 33 %. False alerts were all
    raised at dispatch for far stops: a jam on the first leg (speed 4-16 km/h) inflated every
    later ETA.
  - Current leg at recent speed, later legs at 25 km/h: precision 82-86 %, recall over warnable
    late orders 97-100 %, over all late orders 61-69 %; 6-10 re-raises a day. At T-15 the ETA
    error is median 0.7 min (p10 -9.4, p90 10.8).
  - 30-40 % of late orders end their window < 15 min after dispatch (the simulator makes an order
    late by setting its window 5-40 min before the true arrival), so they cannot be warned 15 min
    ahead. Spec question 3, question 5 and AC5 revised with the owner (spec, "Revised during the build").
- **S2 PyFlink job: works, with three changes.**
  - Python UDFs inside the SQL part failed in the planner once their ROW fields fed a union and a
    lookup join (`ClassCastException: RexFieldAccess cannot be cast to RexCall`). So SQL only reads
    raw Kafka bytes and does the depot lookup on the Kafka key (= vehicle id); decoding runs in the
    DataStream part with the same `contracts` functions (`decode_gps`, `decode_cdc`).
  - PyFlink 1.20 has no type that converts to the Table side's `LocalDateTime`
    (`java.sql.Timestamp cannot be cast`), so timestamps cross the DataStream part as epoch ms and
    the sinks convert with `TO_TIMESTAMP_LTZ`.
  - `flink run -py` looks for `flink-python` in `opt/`; the image keeps it there and in `lib/`.
    Files shipped with `-pyfs` land in a work directory, so paths go through `contracts`
    (`SCHEMAS_DIR`). Startup modes: orders `earliest-offset` (open orders = the whole changelog),
    GPS `latest-offset`; a restore uses the checkpointed offsets either way.
  - JDBC: `flink-connector-jdbc 3.3.0-1.20` holds the core; the Postgres dialect is the separate
    `flink-connector-jdbc-postgres` jar. The depot view lives in `serving` (`serving.van_depots`).
- **S3 latency:** ping → `serving.alerts` 1.3-3.7 s, ping → `vehicle_latest` 1.7 s (bundle 100 ms,
  JDBC flush 200 ms), one van, live.
  - Tried and not taken: a lateness margin proportional to time-to-arrival (precision +5-10 pts,
    recall -3-6 pts), a 30 min speed window, 28/30 km/h typical speed (fits the simulator, not a fleet).

## Departures from the plan (build, 2026-11-19)
- **AC8 tolerance, made explicit** (refinement 3). The demo day with default chaos gives the same
  alerts except borderline SLA cases: after an offline burst the flushed pings arrive *after* the
  order changes that happened meanwhile, so their ETAs use a route without the stop just delivered.
  The test allows an SLA alert in one run only if the other run came within 2 min of the threshold
  (at most 5 % of alerts), and a raise up to 2 min earlier or 15 min later.
- **Still periods must be continuous.** The chaos replay found a false abnormal stop: two short
  stops around a 13 min offline burst were joined into one 16 min "stop" at a place whose order was
  already delivered. A gap over 2 min between pings, or a move over 150 m, now starts a new period.
- **Out-of-order pings count for the speed window** (they still never move the van back).
- **Open orders without a position** are listed in `order_eta` with no ETA (found by comparing the
  DB with `serving.order_eta` after a fresh start).
- The depot view is `serving.van_depots` (not `public`): it belongs to the serving layer.

## Design
**Simulator** (`simulator/day.py`, `simulator/__main__.py`): `simulate_day(..., abnormal_stops=0)`
picks vans by seed, splits their return hop at a point > 150 m from every stop and the depot, and
inserts a 12–25 min speed-0 segment; the depot arrival moves later. Exposed as `day.abnormal_stops`
(van, lat, lon, start, minutes). CLI `--abnormal-stops N`; `make demo` uses `--abnormal-stops 2`.

**Pure logic** (`flink/udf/`, unit tested without Flink):
- `eta.py`: `haversine_km`, `route_eta(now, pos, current_speed_kmh, stops_ahead, typical=25 km/h, dwell=4 min)`.
- `tower.py`: `VanState` (latest ping, last 10 min of moving speeds, open orders by stop_seq, stop
  start, alert state) and `step(state, event, now) -> outputs`:
  - ping newer than latest → update position, recompute ETAs of out-for-delivery orders, evaluate
    SLA risk (ETA ≥ promised_end + 1 min; once; re-raise only ≥ 10 min after resolve; resolve when
    ETA ≤ promised_end) and abnormal stop (< 3 km/h for ≥ 10 min, > 150 m from open stops and depot;
    resolve on moving);
  - older or duplicate ping → ignored;
  - order change → add/update/remove the order (delivered/failed resolve its alert; a van change
    removes it from the old van's state via the before image);
  - alert outputs only when `now − event_ts ≤ horizon`; alert id = `type:subject:raise event_ts`
    (deterministic, so a replay upserts the same row).

**Flink job `control_tower`** (`flink/jobs/control_tower.py`, PyFlink Table + DataStream):
- Kafka raw sources: GPS (decoded with the vectorized `contracts.udf.decode_gps_udf`, rejects dropped:
  bronze already dead-letters them) and `cdc.public.orders` (`decode_orders_cdc_udf`).
- GPS ⋈ `public.van_depots` (JDBC lookup, cache 1 h) adds depot lat/lon.
- Union into one event row → `to_data_stream` → `key_by(van_id)` → `KeyedProcessFunction` (state:
  one pickled `VanState`, RocksDB) → three tagged output streams → `from_data_stream`.
- Statement set: JDBC upsert sinks `serving.vehicle_latest`, `serving.order_eta`, `serving.alerts`
  (primary keys; deletes for closed orders); Kafka sink `alerts.v1` (exactly-once, values encoded
  with the registry id by a small Python UDF using `contracts`).
- `python.fn-execution.bundle.time` 100 ms for latency; parallelism 1 (state is ~200 vans).

**Serving DB:** `infra/local/postgres/serving.sql` (idempotent: schema, 3 tables with `written_at`
set by trigger, the `public.van_depots` view), applied by a new one-shot `serving-init` service on
every `make up` (init scripts only run on an empty volume). Postgres leaves the `cdc` profile.

**Kafka:** `alerts.v1` (3 partitions, bounds as ADR 0012) in `init.sh`; schema
`schemas/alerts.v1-value/v1.avsc` (registered and compat-checked like GPS, ADR 0008).

**Submit:** `submit.sh` runs `.py` jobs with `flink run -d -py` (savepoint/checkpoint resume via
`-s`, same pending/jobids logic); `flink-submit` mounts `flink/` into `/opt/ct/flink` so the cluster
imports `flink.udf.*`. Dockerfile adds `flink-connector-jdbc 3.3.0-1.20` (+ core/postgres modules if
needed) and `postgresql 42.7.5`, SHA-1 pinned. Task slots 6 → 7.

**Status:** `make tower-status` (psql): open orders with ETA, open alerts by type, newest ping per van.

## Files that change
- **New:** `flink/__init__.py`, `flink/udf/{__init__,eta,tower}.py`, `flink/jobs/control_tower.py`,
  `schemas/alerts.v1-value/v1.avsc`, `infra/local/postgres/serving.sql`, `infra/local/postgres/tower_status.sql`,
  `docs/decisions/0013-control-tower-keyed-state.md`, `tests/unit/{test_eta,test_tower,test_tower_replay}.py`,
  `tests/integration/{test_control_tower,test_control_tower_restart}.py`, `specs/006-control-tower/{plan,tasks}.md`.
- **Changed:** `simulator/{day,__main__}.py` (+ `tests/unit/test_day.py`), `infra/local/flink/{Dockerfile,submit.sh}`,
  `infra/local/docker-compose.yml`, `infra/local/redpanda/init.sh`, `Makefile`, `tests/integration/conftest.py`.
- **Docs:** ADR 0013; `docs/requirements.md` (join rule, BR-3 refined, alert horizon measured);
  `docs/project-spec.md`, `docs/roadmap.md`, `README.md`, the Flink rules.

## Order of work
1. **Spikes:**
   - S1 (pure, an hour): offline replay of the default simulated day with a first `tower.py` →
     recall/precision. If below target, change the spec before building further (spec AC5 rule).
   - S2: a minimal Python job (Kafka raw → UDF → DataStream keyed state → JDBC upsert + Kafka) submitted
     with `flink run -py`, stopped with a savepoint and resumed; JDBC jars load.
   - S3: ping → Postgres latency with bundle time 100 ms.
2. Simulator abnormal stops (AC1).
3. `eta.py`, `tower.py` with unit tests; offline replay tests (AC2 formula, AC3, AC4, AC5, AC6, AC8).
4. Serving DDL, `alerts.v1` schema/topic, Dockerfile jars, `submit.sh` for `.py`, compose.
5. `control_tower.py` job; e2e AC2, AC3, AC4, AC6, AC7, AC10, AC11.
6. Restart e2e AC9; memory AC12.
7. ADR 0013, docs; `make check`, `make e2e`, `make e2e-restart`; spec Implemented; PR.

## Risks and rollback
- **R1 Targets not met** (S1). The heuristic ignores real roads; the simulator drives straight lines,
  so it should be close. If not, the spec targets change with the owner before the PR.
- **R2 PyFlink Table↔DataStream with keyed state + JDBC** (S2). Fallback: the job writes outputs to
  Kafka only and a small Kafka→JDBC SQL job upserts Postgres.
- **R3 Latency** (S3): Python bundle buffering. Fallback: bundle size 1 / time 50 ms; JDBC flush 200 ms.
- **R4 Memory:** one more Python worker (~300 MB). Measured by AC12; fallback taskmanager +256 MB.
- **R5 Shared test vans:** orders reference real `VAN-*` rows (FK). Each e2e test uses its own van
  from a reserved range (`VAN-0181`–`VAN-0200`) and closes its orders at teardown, so no state leaks.
- **Rollback:** revert the branch; `DROP SCHEMA serving CASCADE`; delete topic `alerts.v1`. No
  existing table or job changes behaviour.

## Proof
| AC | Test |
|---|---|
| AC1 abnormal stops injected | unit `test_day.py`: count, place > 150 m from stops/depot, length 12–25 min, orders identical to clean |
| AC2 ETA formula + 10 s | unit `test_eta.py`, `test_tower.py`; e2e `test_open_order_gets_an_eta_within_10s` |
| AC3 older ping ignored | unit `test_tower.py`; e2e `test_late_ping_does_not_move_the_van_back` |
| AC4 SLA risk once, re-raise, resolve | unit `test_tower.py`; e2e `test_sla_risk_raised_once_and_resolved_on_delivery` |
| AC5 recall/precision | unit `test_tower_replay.py` on the default simulated day |
| AC6 abnormal stop, no false alerts | unit replay (injected = alerted, clean day 0); e2e `test_abnormal_stop_alert` |
| AC7 p95 < 10 s | e2e `test_alert_latency_p95_under_10s` (10 live raises) |
| AC8 clean = chaos | unit `test_tower_replay.py` (same simulated day, `simulator.chaos`) |
| AC9 catch-up | restart e2e `test_catch_up_raises_no_stale_alert_and_none_twice` |
| AC10 alerts.v1 once | e2e: read_committed consumer, decode, one message per (alert, event) |
| AC11 status | e2e `make -s tower-status` vs direct queries |
| AC12 memory | existing memory e2e, < 6 GB |

## Verification
- `make check` (simulator, ETA, state machine, replay recall/precision, clean = chaos).
- `make up`, `make demo` (with abnormal stops), `make tower-status`: ETAs move, SLA-risk and
  abnormal-stop alerts appear; `psql` on `serving.alerts`; consume `alerts.v1`.
- `make e2e`, `make e2e-restart`, `make stack-mem` < 6 GB.

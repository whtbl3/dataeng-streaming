# Project spec: Delivery Control Tower
Status: approved (2026-11-17) · Derived from: docs/intent.md (approved 2026-11-17)

## 1. Goal
Give dispatchers a live view of every in-flight delivery, with an alert **before** an
order breaches its promised window. Give managers trustworthy delivery KPIs within
minutes instead of the next day. The stack runs locally for $0 and deploys to AWS managed
services on demand (ADR 0005); it is designed and benchmarked for ~100M events (ADR 0009).

## 2. Scope
**In:** synthetic fleet + order simulator; CDC from the order database; streaming
ingestion, dedup and late-data handling; live ETA; SLA-risk and abnormal-stop
alerts; Iceberg lakehouse (bronze/silver/gold); dbt KPIs with data-quality tests;
Grafana dashboards (ops + pipeline health); Terraform for AWS; CI.
**Out:** route optimisation, driver/customer apps, ML ETA, real data, HA/multi-region.

## 3. Architecture (box level)
```
 ┌───────────── Simulator (HCMC, 200 vans) ─────────────┐
 │ GPS pings (every 5 s)          Order lifecycle writes │
 └──────┬───────────────────────────────────┬────────────┘
        │ produce (Avro)                    │ INSERT/UPDATE
        v                                   v
 ┌──────────────┐   Debezium CDC     ┌──────────────┐
 │ Kafka topics │ <───────────────── │ Postgres OLTP│
 │ gps / orders │                    └──────────────┘
 └──────┬───────┘
        v
 ┌───────────────────────── Flink ─────────────────────────┐
 │ A. ingest: validate → DLQ, raw → bronze (Iceberg)        │
 │ B. clean: dedup + event-time watermarks → silver         │
 │ C. control tower: latest position ⋈ active orders        │
 │    → ETA, SLA-risk alert, abnormal-stop alert            │
 └──────┬────────────────────────────┬─────────────────────┘
        v                            v
 ┌───────────────────┐       ┌─────────────────────────────┐
 │ Iceberg lakehouse │       │ Serving: Postgres tables     │
 │ bronze/silver     │       │ vehicle_latest, order_eta,   │
 │   → dbt → gold    │       │ alerts  (+ alerts topic)     │
 └────────┬──────────┘       └──────────────┬──────────────┘
          v  Trino / Athena                 v
       ┌──────────────── Grafana ────────────────┐
       │ live map · alerts · KPIs · pipeline health│
       └──────────────────────────────────────────┘
```
Design principle (ADR 0006): **the stream gives freshness, the batch (dbt) gives
correctness.** Alerts are computed per event in Flink. Gold KPIs are recomputed by
dbt from deduplicated silver tables: every 5 min over a 2 h look-back (*provisional*),
and nightly for the whole previous day (*final*, "close the books"). Late or duplicate
events therefore cannot skew the final numbers.

## 4. Key data entities
| Entity | Grain | Source | Notes |
|---|---|---|---|
| `gps_ping` | vehicle × event_time | simulator → Kafka | lat, lon, speed, heading; may be late/duplicated |
| `order` | order_id (versioned) | Postgres → CDC | depot, destination, promised window, vehicle_id, stop_seq, status |
| `order_status_event` | order_id × change | CDC | created → assigned → out_for_delivery → delivered / failed |
| `vehicle` / `depot` | id | seed data | static dimensions |
| `order_eta` | order_id (latest) | Flink | predicted arrival, distance remaining, updated_at |
| `alert` | alert_id | Flink | type (SLA_RISK, ABNORMAL_STOP), order/vehicle, raised_at, resolved_at |
| `fct_delivery` | delivered order | dbt gold | on_time flag, minutes late, ETA error at T-30/T-15 min |
| `agg_depot_daily` | depot × day | dbt gold | on-time %, stops/hour, alert precision, `is_final` |
| `closed_days` | local day | dbt nightly | closed_at, late_events_absorbed, late_after_close |

## 5. Interfaces
- **Kafka topics:** `gps.pings.v1`, `cdc.public.orders`, `alerts.v1`, `dlq.*`. Avro schemas live in `schemas/`, and Schema Registry enforces BACKWARD compatibility.
- **Lakehouse:** Iceberg namespaces `bronze`, `silver`, `gold`, queried with SQL (Trino locally, Athena on AWS).
  - `bronze.{depots,vans,orders}_cdc` (spec 002): append-only Debezium changelogs: `op` (c/u/r), typed `before`/`after` rows (full before-images for orders), `source_lsn`, `source_txid`, `commit_ts`, `capture_ts`, Kafka partition/offset, `ingest_ts`; `orders_cdc` partitioned by `day(commit_ts)`; Flink job `bronze_cdc`.
  - `bronze.gps_pings` (spec 001): raw ping fields + `ingest_ts` (Kafka record time) + `kafka_partition`/`kafka_offset`; partitioned by `day(event_ts)`; written by Flink job `bronze_gps` (exactly-once, commit every 30 s checkpoint).
- **Order DB (CDC source, spec 002):** Postgres `public.{depots,vans,orders}`, publication `ct_cdc`, slot `ct_cdc_slot`; written by `python -m simulator day`.
- **Serving DB:** Postgres schema `serving` (read by Grafana).
- **CLI / Make:** `make check`, `make up [PROFILES=...]`, `make demo`, `make backfill DAYS=N`, `make bench`, `make down`; `make aws-up|aws-down` (managed).

## 6. Non-functional requirements
- **Latency:** GPS event → alert in `serving.alerts`, p95 < 10 s (local, live load).
- **Correctness:** events up to 2 h late or duplicated produce identical intraday KPIs; any
  event that arrives before the nightly close produces identical *final* KPIs (tests).
- **Delivery semantics:** Flink checkpoints enabled; Iceberg sink exactly-once; serving
  tables use idempotent upserts.
- **Volume:** `standard` tier = 30 days ≈ 100M GPS events (ADR 0009). Iceberg tables
  partitioned by `day(event_ts)` (+ `bucket(vehicle_id)` for GPS), with scheduled compaction.
- **Throughput:** measured by `make bench` with the hardware recorded; initial goal ≥ 20k
  events/s for backfill on the laptop; `stress` tier without unbounded consumer lag.
- **Local footprint:** full stack ≤ 8 GB RAM; compose profiles (`core`, `cdc`, `ui`) to run less.
- **Cost:** local $0; managed demo session < $5; all resources tagged and destroyable in one command.
- **Security:** no secrets in git; least-privilege IAM; RDS in private subnets.
- **Reproducibility:** fresh clone → running demo in < 10 min with Docker only; deterministic simulator seed.

Business definitions (BR-*), non-functional requirements (NFR-*) and data-modeling rules live in
[docs/requirements.md](requirements.md). An interactive walkthrough of the design is in
`docs/design/system-design-walkthrough.html` (open it in a browser).

## 7. Definition of Done
- [ ] `make check` passes (same as CI)
- [ ] Every acceptance criterion has a test
- [ ] Docs and this spec updated
- [ ] No secrets in the diff

## 8. Decision log
| ADR | Decision | Status |
|---|---|---|
| [0001](decisions/0001-kafka-redpanda-msk-serverless.md) | Kafka API: Redpanda local, MSK Serverless on AWS | Final |
| [0002](decisions/0002-apache-flink-stream-processing.md) | Apache Flink (SQL first) / Managed Service for Apache Flink | Final |
| [0003](decisions/0003-iceberg-lakehouse.md) | Apache Iceberg; REST catalog + Trino local, Glue + Athena on AWS | Final |
| [0004](decisions/0004-cdc-postgres-debezium.md) | Order events via Postgres + Debezium CDC | Final |
| [0005](decisions/0005-local-first-aws-on-demand.md) | Local-first, AWS managed services on demand | Final |
| [0006](decisions/0006-stream-freshness-batch-correctness.md) | Stream for freshness, dbt batch for correct KPIs | Final |
| [0007](decisions/0007-grafana-postgres-serving.md) | Grafana + Postgres serving tables | Final |
| [0008](decisions/0008-avro-schema-registry.md) | Avro + Schema Registry (Glue SR on AWS) | Proposed |
| [0009](decisions/0009-data-tiers-and-benchmark.md) | Data tiers (dev / standard 100M / stress) + benchmark | Final |

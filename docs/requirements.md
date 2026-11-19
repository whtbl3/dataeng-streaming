# Requirements: Delivery Control Tower

> **Status:** Approved (2026-11-18) · **Derived from:** [intent.md](intent.md) (approved)
> Interactive explanation: `docs/design/system-design-walkthrough.html`

This document fixes the business definitions, non-functional requirements and data-modeling
rules **before** features 3–11 are specified. Every item has an ID, so specs, ADRs and tests
can reference it (for example, "covers BR-2, NFR-6").

**Contents**
1. [At a glance](#at-a-glance)
2. [Business definitions (BR)](#1-business-definitions-br)
3. [Non-functional requirements (NFR)](#2-non-functional-requirements-nfr)
4. [Data-modeling rules](#3-data-modeling-rules)
5. [Planned tables](#4-planned-tables)
6. [Traceability](#5-traceability)
7. [Resolved questions](#6-resolved-questions)

---

## At a glance

| ID | Name | In one line |
|---|---|---|
| BR-1 | Late delivery | Delivered after `promised_end`. |
| BR-2 | On-time rate | On time ÷ (delivered + failed). |
| BR-3 | At-risk order | ETA ≥ 1 min past `promised_end`; one alert per order. |
| BR-4 | Business day | Local day in Asia/Ho_Chi_Minh; storage in UTC. |
| BR-5 | Alert quality | Precision and recall of SLA_RISK alerts. |
| NFR-1 | Freshness | Alert p95 < 10 s; KPIs ≤ 5 min; final after 01:00. |
| NFR-2 | Correctness | Late or duplicate events do not change KPIs. |
| NFR-3 | RPO | 0 events lost. |
| NFR-4 | RTO | Alerts back within 15 min of a restart. |
| NFR-5 | Retention | 7 days or a size bound (Kafka) to 2 years (gold). |
| NFR-6 | Privacy | Customer coordinates never reach gold. |
| NFR-7 | Security (AWS) | Least privilege, KMS, private subnets. |
| NFR-8 | Availability | No HA commitment. |
| NFR-9 | Cost | $0 local; < $5 per AWS demo. |
| NFR-10 | Observability | Alerts on lag, checkpoints, slot, silence. |
| NFR-11 | Volume | Designed for ≈ 100M GPS events (30 days); ADR 0012. |

All items are **Approved** unless their section says **Final** (already fixed by the intent,
an ADR or an implemented spec).

---

## 1. Business definitions (BR)

### BR-1 · Late delivery — *Final (spec 002)*
A delivered order is late when `closed_at > promised_end`.

### BR-2 · On-time rate
- **Formula:** `on-time deliveries / (delivered + failed)`.
- A failed order counts as **not on time**.
- The failure rate is reported separately.

### BR-3 · At-risk order — *Final (spec 006, ADR 0013)*
- An open order whose predicted ETA is later than `promised_end`.
- **Fires** when the ETA exceeds `promised_end` by ≥ 1 min.
- **Once per order.** It is raised again only after it was resolved for ≥ 10 min.
- **Resolved** when the ETA is back at or before `promised_end`, or the order is delivered or failed.
- The ETA exists from dispatch (out for delivery); an order whose window ends less than 15 min
  after dispatch cannot be warned 15 min ahead (BR-5 recall counts the orders that can).

### BR-4 · Business day
- The local calendar day in **Asia/Ho_Chi_Minh** (UTC+7).
- All timestamps are **stored in UTC**.
- Daily KPIs, gold partitions and the nightly close use the **local** day.

### BR-5 · Alert quality
- **Precision:** the share of SLA_RISK alerts whose order was actually late.
- **Recall:** the share of late orders that got an alert ≥ 15 min before `promised_end`.

---

## 2. Non-functional requirements (NFR)

### NFR-1 · Freshness — *Final (intent, ADR 0006)*
| Output | Target |
|---|---|
| Alert (GPS event → `serving.alerts`) | p95 < 10 s |
| Bronze tables | ≤ 60 s |
| Provisional KPIs | ≤ 5 min + run time |
| Final KPIs | after the nightly close (01:00 local) |

### NFR-2 · Correctness — *Final*
Duplicate or late events give **identical gold KPIs**:
- **intraday**, for events up to 2 h late;
- **at the nightly close**, for anything that arrived that day.

### NFR-3 · RPO (data loss)
- **0 events lost.**
- Kafka keeps 7 days.
- The Postgres WAL is kept by the replication slot (capped at 1 GB).

### NFR-4 · RTO (recovery)
- The alert path is back within **15 min** of a restart.
- During catch-up, events older than the **alert horizon** (default 10 min) update state but
  **do not raise alerts**.

### NFR-5 · Retention
| Data | Kept for |
|---|---|
| Kafka topics | 7 days, or a size bound per partition (GPS 1 GiB, CDC 256 MiB, DLQ 128 MiB), whichever first |
| bronze GPS | 90 days |
| bronze CDC | 1 year |
| silver | 1 year |
| gold | 2 years |
| Iceberg snapshots | 7 days (scheduled `expire_snapshots`); orphan files 7 days |

Enforced by the maintenance service (spec 005, ADR 0012); partitions are dropped whole.

### NFR-6 · Privacy (PII)
- `dest_lat` and `dest_lon` are personal data (a customer address).
- They are read-restricted in bronze and silver.
- Gold stores only `dest_area`: a Bing tile quadkey at zoom 18 (≈ 150 m; Trino and Athena have it
  built in, unlike geohash; spec 007).

### NFR-7 · Security on AWS — *feature 10*
- Least-privilege IAM per service.
- KMS encryption for S3, MSK and RDS.
- RDS in private subnets.
- No credentials in git (already enforced by hooks).

### NFR-8 · Availability — *Final*
No HA commitment (demo project; see "Out of scope" in the intent).

### NFR-9 · Cost — *Final (ADR 0005)*
- Local: $0.
- Managed demo: < $5 per session, destroyed afterwards.
- Budget alerts at $20 and $50.

### NFR-10 · Observability — *feature 8*
Alert when:
- consumer lag grows for > 5 min;
- a checkpoint fails;
- replication slot lag > 500 MB;
- bronze receives no rows for 2 min.

### NFR-11 · Volume — *Final (ADR 0009, 0012)*
- Designed for the `standard` tier: ≈ 100M GPS events over 30 days. Spec 005 removed the limits
  per layer (ADR 0012); a measured backfill and benchmark are a stretch item.
- Measured event sizes replace the estimates. The CDC size (≈ 1.5 KB) is not yet measured.

---

## 3. Data-modeling rules

The stream never has "complete" data, so the model must say what is complete.
These rules apply to every table from silver onward. A batch-only design could skip them.

1. **Events first, state derived.**
   - Bronze is the append-only source of truth.
   - Every silver and gold table can be rebuilt from bronze.
   - Replay gives identical results (idempotent writes).
2. **Every table declares its grain, dedup key and ordering key.**
   - Kafka offsets are never keys: they change on replay.
   - "Last write wins" is decided by the ordering key (`source_lsn`, `event_ts`), never by arrival order.
3. **Two time axes.**
   - **Business time** (`event_ts`, `status_ts`, `closed_at`) is used for KPIs.
   - **System time** (`ingest_ts`, `commit_ts`) is used for lateness, debugging and "what did we know at T?".
4. **Completeness is explicit.**
   - Gold rows carry `is_final`.
   - `gold.closed_days` records each close and counts late arrivals after it (`late_after_close`).
   - A closed day changes only through an explicit, logged restatement (`make reclose DAY=…`).
5. **One write mode per layer.**
   - The batch part reads the same bronze and silver tables as the stream.
   - This is a hybrid on one log, not a Lambda architecture with two code bases.

| Layer | Written by | Write mode |
|---|---|---|
| bronze | Flink | append-only, exactly-once |
| silver | Flink or dbt | MERGE on dedup key, guarded by the ordering key |
| gold (provisional) | dbt, every 5 min | incremental MERGE of the orders changed in the last 2 h of commit time (spec 007, ADR 0014) |
| gold (final) | dbt, nightly | overwrite the closed day's partitions |
| serving | Flink | upsert by key, only if the event is newer |

---

## 4. Planned tables

### Silver (feature 4)
| Table | One row = | Dedup key | Ordering key | Partition |
|---|---|---|---|---|
| `silver.gps_pings` | a unique ping | `(vehicle_id, event_ts)` | `event_ts` | `day(event_ts)`, `bucket(16, vehicle_id)` |
| `silver.orders` | an order, latest state | `order_id` (stored key `(order_id, created_at)`) | `last_lsn` | `month(created_at)` |
| `silver.order_status_history` | a status reached | `(order_id, status)` (stored key adds `status_ts`) | `source_lsn` (first) | `day(status_ts)` |

> Iceberg upserts need the partition source column in the key, so the stored keys add
> `created_at` / `status_ts`. Both are set once per order or status, so the grain is unchanged
> (spec 004, ADR 0011). For GPS copies, the first arrival (smallest `ingest_ts`) wins.

### Gold (spec 007, ADR 0014)
| Table | One row = | Key | Partition |
|---|---|---|---|
| `gold.fct_delivery` | a closed order | `order_id` | `local_date` |
| `gold.dim_van` | a version of a van (SCD Type 2 from CDC) | `van_key` (surrogate); ordered by `valid_from` | — |
| `gold.dim_depot` | a depot | `depot_id` | — |
| `gold.dim_date` | a local day | `local_date` | — |
| `gold.agg_depot_daily` | depot × local day | `(depot_id, local_date)` | `local_date` |
| `gold.closed_days` | a closed local day | `local_date` | — |

### Serving (spec 006, Postgres)
| Table | One row = | Key | Ordering key |
|---|---|---|---|
| `serving.vehicle_latest` | a van | `van_id` | `event_ts` |
| `serving.order_eta` | an open order | `order_id` | `updated_at` |
| `serving.alerts` | an alert | `alert_id` | `raised_at` |

### Columns of `gold.fct_delivery`
| Group | Columns |
|---|---|
| Identifiers | `order_id`, `local_date`, `depot_id`, `van_key` (the `dim_van` version as of `closed_at`) |
| Times and outcome | `promised_start`, `promised_end`, `closed_at`, `outcome` |
| Results | `is_on_time` (BR-2), `minutes_late`, `eta_error_t30_min`, `eta_error_t15_min` |
| Alerts | `first_alert_at`, `alert_lead_min` |
| Other | `dest_area` (NFR-6), `is_final` |

> **Join rule:** facts join dimensions **as of the event time**, never on the current row.
> `dim_van` versions are matched on `closed_at`. The alert path keeps one state per van
> (positions + open orders, ADR 0013) instead of a temporal join: a temporal join must key on
> the order, and "idle for 10 min" needs the van's own history.

---

## 5. Traceability

Each business question points to the requirement that answers it, the design that implements
it, and the test that proves it.

**How fast must dispatchers know?**
- Requirement: NFR-1
- Design: Flink alert path + Postgres serving (ADR 0006, 0007)
- Proof: spec 006 `test_alert_latency_p95_under_10s` (p95 1.5 s locally)

**Do late or duplicate events change the numbers?**
- Requirement: NFR-2, rules 2 and 4
- Design: dedup keys, 2 h look-back, nightly close (ADR 0006)
- Proof: spec 001 AC3, spec 002 AC5 and AC7, spec 007 `test_dirty_cdc_gives_the_same_gold_kpis_as_clean`

**Does a failed order count as late?**
- Requirement: BR-2
- Design: `fct_delivery.is_on_time`
- Proof: dbt unit test `fct_delivery_outcomes_eta_error_alert_lead_and_van_version` (spec 007)

**Which day does 06:30 local belong to?**
- Requirement: BR-4
- Design: `local_date` partitions in gold
- Proof: dbt unit test `stg_orders_closed_local_day_at_midnight` (spec 007 AC5)

**Can we lose data if the pipeline stops?**
- Requirement: NFR-3, NFR-4
- Design: Kafka retention, replication slot, alert horizon
- Proof: e2e restart tests (spec 002 AC7; spec 006 `test_catch_up_raises_no_stale_alert_and_none_twice`)

**How long do we keep history?**
- Requirement: NFR-5
- Design: retention settings + snapshot expiry jobs (maintenance service, ADR 0012)
- Proof: spec 005 e2e (`test_maintenance.py`: expiry, orphans, retention; `test_scale.py`: Kafka bounds)

**Is the customer's location protected?**
- Requirement: NFR-6
- Design: `dest_area` in gold; bronze and silver restricted
- Proof: dbt test `gold_has_no_exact_destination` (spec 007 AC3)

**Does a van changing depot rewrite history?**
- Requirement: rule 2, `dim_van` SCD2
- Design: SCD2 built from `bronze.vans_cdc`
- Proof: dbt test (feature 7)

---

## 6. Resolved questions
*Approved 2026-11-18.*

1. **Alert horizon during catch-up:** 10 min (processing time − event time), spec 006 AC9.
2. **When ETA error is measured:** at T-30 and T-15 min **before `promised_end`**, when the dispatcher acts.
3. **Provisional numbers on dashboards:** show the latest value with a visible "provisional"
   marker; reports use final-only views.

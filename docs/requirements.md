# Requirements: Delivery Control Tower
Status: **Approved (2026-11-18)** · Date: 2026-11-18 · Derived from: docs/intent.md (approved)

This document closes the requirement gaps found in the design walkthrough
(`docs/design/system-design-walkthrough.html`). It fixes business definitions,
non-functional requirements and data-modeling rules **before** features 3–11 are specified.
Every item has an ID so that specs, ADRs and tests can reference it.

## 1. Business definitions
| ID | Term | Definition | Status |
|---|---|---|---|
| BR-1 | Late delivery | `closed_at > promised_end` for a delivered order. | Final (spec 002) |
| BR-2 | On-time rate | `on-time deliveries / (delivered + failed)`. A failed order counts as **not on time**; the failure rate is reported separately. | Approved |
| BR-3 | At-risk order | An open order whose predicted ETA is later than `promised_end`. The alert fires when the ETA exceeds `promised_end` by ≥ 1 min, and it fires once per order. It is re-raised only after it was resolved for ≥ 10 min. | Approved (feature 6 refines) |
| BR-4 | Business day | The local calendar day in **Asia/Ho_Chi_Minh** (UTC+7). All timestamps are stored in UTC; daily KPIs, partitions in gold and the nightly close use the local day. | Approved |
| BR-5 | Alert quality | Precision = share of SLA_RISK alerts whose order was actually late. Recall = share of late orders that got an alert ≥ 15 min before `promised_end`. | Approved |

## 2. Non-functional requirements
| ID | Area | Requirement | Status |
|---|---|---|---|
| NFR-1 | Freshness | Alert path p95 < 10 s (GPS event → `serving.alerts`); bronze ≤ 60 s; provisional KPIs ≤ 5 min + run time; final KPIs after the nightly close (01:00 local). | Final (intent, ADR 0006) |
| NFR-2 | Correctness | Duplicate or late events give identical gold KPIs: intraday for events ≤ 2 h late, at the nightly close for anything that arrived that day. | Final |
| NFR-3 | RPO | **0 events lost**. Kafka keeps 7 days; the Postgres WAL is kept by the replication slot (capped at 1 GB). | Approved |
| NFR-4 | RTO | Alert path back within **15 min** of a restart. On catch-up, events older than the alert horizon (default 10 min) update state but **do not raise alerts**. | Approved |
| NFR-5 | Retention | Kafka 7 days · bronze GPS 90 days · bronze CDC 1 year · silver 1 year · gold 2 years. Iceberg snapshots expire after 7 days (scheduled `expire_snapshots`). | Approved |
| NFR-6 | Privacy (PII) | `dest_lat`/`dest_lon` are personal data (a customer address). They are read-restricted in bronze/silver. Gold stores only `dest_area` (geohash, 7 characters, ≈ 150 m). | Approved |
| NFR-7 | Security (AWS) | Least-privilege IAM per service; KMS encryption for S3, MSK and RDS; RDS in private subnets; no credentials in git (already enforced by hooks). | Approved (feature 10) |
| NFR-8 | Availability | No HA commitment (demo project; see intent "Out of scope"). | Final |
| NFR-9 | Cost | Local $0. A managed demo is < $5 per session and is destroyed afterwards. Budget alerts at $20 and $50. | Final (ADR 0005) |
| NFR-10 | Observability | Alert when consumer lag grows for > 5 min, a checkpoint fails, slot lag > 500 MB, or bronze receives no rows for 2 min. | Approved (feature 8) |
| NFR-11 | Volume | Designed and benchmarked for the `standard` tier (≈ 100M GPS events, 30 days). Measured event sizes replace the estimates (CDC ≈ 1.5 KB is not yet measured). | Final (ADR 0009); measurement in feature 5 |

## 3. Data-modeling rules (streaming + batch)
The stream never has "complete" data, so the model must say what is complete. These rules apply
to every table from silver onward. A batch-only design could skip them.

1. **Events first, state derived.** Bronze is the append-only source of truth. Every silver and gold table can be rebuilt from bronze, and replay must give identical results (idempotent writes).
2. **Every table declares its grain, its dedup key and its ordering key.** Offsets are never keys, because they change on replay. "Last write wins" is decided by the ordering key (`source_lsn`, `event_ts`), never by arrival order.
3. **Two time axes.** Business time (`event_ts`, `status_ts`, `closed_at`) is used for KPIs. System time (`ingest_ts`, `commit_ts`) is used for lateness, debugging and "what did we know at T" questions.
4. **Completeness is explicit.** Gold rows carry `is_final`. `gold.closed_days` records each close and counts late arrivals after the close (`late_after_close`). A closed day changes only through an explicit, logged restatement (`make reclose DAY=…`).
5. **One write mode per layer**, as in the table below. The batch part reads the same bronze/silver tables as the stream (a hybrid on one log, not a Lambda architecture with two code bases).

| Layer | Written by | Write mode |
|---|---|---|
| bronze | Flink | append-only, exactly-once |
| silver | Flink or dbt | MERGE on dedup key, guarded by the ordering key |
| gold (provisional) | dbt, every 5 min | incremental MERGE over a 2 h look-back |
| gold (final) | dbt, nightly | overwrite the closed day's partitions |
| serving | Flink | upsert by key, only if the event is newer |

## 4. Proposed tables (silver, gold, serving)
| Table | Grain | Key (dedup) | Ordering key | Partition | Feature |
|---|---|---|---|---|---|
| `silver.gps_pings` | 1 unique ping | `(vehicle_id, event_ts)` | `event_ts` | `day(event_ts)`, `bucket(16, vehicle_id)` | 4 |
| `silver.orders` | 1 order, latest state | `order_id` | `last_lsn` | `month(created_at)` | 4 |
| `silver.order_status_history` | 1 status change | `(order_id, status)` | `source_lsn` | `day(status_ts)` | 4 |
| `gold.fct_delivery` | 1 closed order | `order_id` | — | `local_date` | 7 |
| `gold.dim_van` | 1 version of a van (SCD Type 2 from CDC) | `van_key` (surrogate) | `valid_from` | — | 7 |
| `gold.dim_depot`, `gold.dim_date` | 1 depot / 1 local day | natural key | — | — | 7 |
| `gold.agg_depot_daily` | depot × local day | `(depot_id, local_date)` | — | `local_date` | 7 |
| `gold.closed_days` | 1 closed local day | `local_date` | — | — | 7 |
| `serving.vehicle_latest` | 1 van | `van_id` | `event_ts` | — | 6 |
| `serving.order_eta` | 1 open order | `order_id` | `updated_at` | — | 6 |
| `serving.alerts` | 1 alert | `alert_id` | `raised_at` | — | 6 |

**Columns of `gold.fct_delivery`:**
- identifiers: `order_id`, `local_date`, `depot_id`, `van_key` (dim_van as of `closed_at`);
- times and outcome: `promised_start`, `promised_end`, `closed_at`, `outcome`;
- results: `is_on_time` (BR-2), `minutes_late`, `eta_error_t30_min`, `eta_error_t15_min`;
- alerts: `first_alert_at`, `alert_lead_min`;
- other: `dest_area` (NFR-6), `is_final`.

**Join rule:** facts join dimensions **as of the event time**. That means `dim_van` versions are matched on `closed_at`, and the alert path uses a temporal join. They never join on the current row.

## 5. Traceability (question → requirement → design → proof)
| Business question | Requirement | Design | Proof |
|---|---|---|---|
| How fast must dispatchers know? | NFR-1 | Flink alert path + Postgres serving (ADR 0006, 0007) | feature 6 latency test |
| Do late or duplicate events change numbers? | NFR-2, rules 2 and 4 | dedup keys, look-back, nightly close (ADR 0006) | specs 001 AC3, 002 AC5/AC7; feature 7 tests |
| Does a failed order count as late? | BR-2 | `fct_delivery.is_on_time` | dbt test (feature 7) |
| Which day does 06:30 local belong to? | BR-4 | `local_date` partitions in gold | dbt test with events around 00:00 local (feature 7) |
| Can we lose data if the pipeline stops? | NFR-3, NFR-4 | Kafka retention, replication slot, alert horizon | e2e restart tests (002 AC7; feature 6 catch-up test) |
| How long do we keep history? | NFR-5 | retention + snapshot expiry jobs | feature 5 |
| Is customer location protected? | NFR-6 | `dest_area` in gold, restricted bronze/silver | dbt test: no `dest_lat`/`dest_lon` in gold |
| Does a van changing depot rewrite history? | rule 2, dim_van SCD2 | SCD2 built from `bronze.vans_cdc` | dbt test (feature 7) |

## 6. Resolved questions (approved 2026-11-18)
1. Alert horizon on catch-up: **10 min** for v1; measured in feature 6.
2. ETA error is measured at T-30 and T-15 min **before `promised_end`** (when the dispatcher acts).
3. Dashboards show **the latest value plus a visible "provisional" marker**; reports use final-only views.

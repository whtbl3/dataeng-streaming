# Delivery Control Tower

Real-time last-mile delivery tracking on a streaming lakehouse:
**Kafka → Flink → Iceberg → dbt**, with live ETAs and alerts *before* a delivery is late.
It runs locally on Docker for $0, is designed for ~100M GPS events, and its lakehouse moves to
AWS (S3, Glue, Athena) by configuration only.

> **Status:** features 1–8 done, 9 (AWS lakehouse) in progress. See the [roadmap](docs/roadmap.md).
> All data is synthetic: 200 vans in Ho Chi Minh City, ~5k orders a day, one GPS ping per van every 5 s.

## The problem
A delivery company's dispatchers learn that a parcel is late only **after** its promised window
has passed. Van GPS and order status live in separate systems, and managers get on-time KPIs
from a nightly batch, a day late ([intent](docs/intent.md)).

This project answers three questions:

| Who | Question | Answer in this project |
|---|---|---|
| Dispatcher | Which order will be late, before it is? | A live ETA per order and an SLA-risk alert at least 15 min ahead |
| Ops manager | How did each depot do today and yesterday? | On-time rate, ETA error, stops per hour: provisional every 5 min, final every night |
| Analyst | Can I query clean history with SQL? | Bronze → silver → gold Iceberg tables, the same answer even with late or duplicate data |

## The result in one picture
![Data flow: simulator and Postgres CDC into Kafka, Flink into Iceberg bronze and silver, dbt into gold, a Flink alert job into Postgres serving, and Trino as the query engine](docs/diagrams/data-flow.svg)

| Layer | Local ($0) | AWS managed |
|---|---|---|
| Event log | Redpanda (Kafka API) | Amazon MSK Serverless |
| Data contracts | Avro + Redpanda Schema Registry | AWS Glue Schema Registry |
| CDC | Debezium on Kafka Connect | MSK Connect + RDS PostgreSQL |
| Stream processing | Apache Flink 1.20 (SQL + PyFlink) | Managed Service for Apache Flink |
| Lakehouse | Iceberg on MinIO + REST catalog | Iceberg on S3 + Glue Data Catalog (feature 9) |
| SQL / transforms | Trino + dbt | Athena + dbt (feature 9) |
| Serving / dashboards | Postgres serving, dashboards designed | Grafana on ECS + CloudWatch |

Two ideas hold the design together:
- **Stream for freshness, batch for correctness** ([ADR 0006](docs/decisions/0006-stream-freshness-batch-correctness.md)).
  Flink raises alerts within seconds; dbt recomputes KPIs every 5 minutes (provisional) and
  "closes the books" on each day at night (final).
- **Bronze keeps every raw event, and every later table can be rebuilt from it with the same
  answer.** Writes are keyed by business keys and ordered by event time or commit position,
  never by arrival.

## How it was built, step by step
Each step is one feature with its own spec, plan and tests in [`specs/`](specs/). Each one fixes
a problem the previous step left open.

### 1. Walking skeleton: one thin path through every layer
**Problem:** nothing ran end to end, and integrating many parts at once hides where things break.
**Built:** a GPS simulator → Redpanda topic `gps.pings.v1` → a Flink job → Iceberg `bronze.gps_pings`
on MinIO → queried in Trino, plus `make up`, `make check` and CI.
**Decisions:** Redpanda locally, MSK on AWS ([ADR 0001](docs/decisions/0001-kafka-redpanda-msk-serverless.md));
Flink, SQL first ([0002](docs/decisions/0002-apache-flink-stream-processing.md)); Iceberg
([0003](docs/decisions/0003-iceberg-lakehouse.md)); local first, AWS on demand
([0005](docs/decisions/0005-local-first-aws-on-demand.md)).
**Proof:** exactly N pings sent → exactly N rows in bronze; the whole stack stays under 6 GB of RAM.

### 2. Orders through CDC: know what each van carries
**Problem:** the pipeline knew where vans were, not what they deliver. Orders live in another
team's database, which we must not change.
**Built:** Postgres (depots, vans, orders) and an order simulator whose vans really drive to each
address, so GPS and order status tell the same story. Debezium streams every row change to Kafka
and into `bronze.*_cdc` ([ADR 0004](docs/decisions/0004-cdc-postgres-debezium.md)).

![Lifecycle: created, assigned, out_for_delivery, then delivered or failed (customer absent)](docs/diagrams/order-lifecycle.svg)

### 3. Data contracts: one bad message must not stop 200 vans
**Problem:** one malformed message stopped a bronze job, and any producer could rename a field a
running job depends on.
**Built:** every topic is Avro with a registered schema under FULL_TRANSITIVE compatibility,
checked in CI too ([ADR 0008](docs/decisions/0008-avro-schema-registry.md)). An undecodable or
rule-breaking event goes to `bronze.dlq_events` and `dlq.<topic>` with its raw bytes and a reason;
the job keeps running ([ADR 0010](docs/decisions/0010-dlq-raw-source-python-udf.md)).

![Sequence: a ping goes from the van to Kafka, is decoded by bronze_gps with the Schema Registry, appended to bronze, then upserted to silver by silver_gps; a duplicate is appended to bronze but produces no silver row; a ping outside the city goes to the dead-letter table and topic](docs/diagrams/gps-ping.svg)

### 4. Silver: duplicates, late and out-of-order events
**Problem:** real devices resend pings, lose signal in tunnels and flush old pings later. Counting
bronze rows doubles distance, and "the last row" moves a van backwards.
**Built:** a `--chaos` simulator mode (duplicates, pings up to 2 h late, jitter, offline bursts) and
Flink jobs that build silver: one row per real ping (first arrival wins, then an Iceberg upsert on
`(vehicle_id, event_ts)`), late rows kept and flagged, and the current state of each order ordered
by the database commit position ([ADR 0011](docs/decisions/0011-silver-upsert-first-arrival.md)).

**Example, the tunnel ping.** VAN-0001 records a ping at 08:07:00 in a tunnel, sends it at 08:11:02,
is not sure it got through and sends it again. Bronze keeps both copies; silver keeps one:

| vehicle_id | event_ts | ingest_ts | lateness_s | is_late |
|---|---|---|---|---|
| VAN-0001 | 08:07:00 | 08:11:02 | 242 | true |

**Proof:** the same day sent clean and with chaos gives identical silver; killing a task manager
mid-load leaves silver exact.

![Before and after: duplicate ping → dedup and upsert; replayed CDC → order by commit LSN; late ping → late-data flag; growing state → state TTL](docs/diagrams/patterns-clean.svg)

### 5. Scale by design, not by more machines
**Problem:** the target is ~100M GPS events (30 days), but parts of the design had a ceiling that
more machines would not lift: 3 partitions cap readers at 3, Python decoded one row at a time, state
lived in RAM, and small files and delete files piled up.
**Built** ([ADR 0012](docs/decisions/0012-scale-and-table-maintenance.md)): 6 partitions keyed by van,
a vectorized decode UDF (20k → 28k msg/s), RocksDB state with incremental checkpoints, hash write
distribution, day partitions sorted by van, time + size bounded Kafka retention, and a maintenance
service (compaction, snapshot expiry, orphan removal, retention). Scaling out is now configuration.
*Throughput at 100M events is reasoned from these limits, not measured; the benchmark is a stretch item.*

![Before and after: 3 partitions → 6 partitions keyed by vehicle_id; time-only retention → time and size retention; one Python call per message → vectorized UDF; heap state → RocksDB](docs/diagrams/patterns-scale-stream.svg)

### 6. Control tower: the reason the project exists
**Problem:** clean data existed, but nothing combined a van's live position with the orders it
still carries. No ETA, no early warning, no "van stuck where it should not be".
**Built:** a PyFlink job with one keyed state per van: latest position, open orders, alert state
and idle timers ([ADR 0013](docs/decisions/0013-control-tower-keyed-state.md)). It writes ETAs and
alerts to Postgres serving, to the `alerts.v1` topic (exactly once) and to the lakehouse.
- **ETA v1:** the current leg at the van's recent speed, later stops at 25 km/h. A first version
  used recent speed on every leg and gave 33 % precision (one traffic jam inflated every later
  ETA); the change raised it to 82–86 %, with 97–100 % recall on late orders that can still be
  warned 15 min ahead.
- **Alerts:** SLA risk (ETA past the window by a margin, at most once per order until resolved)
  and abnormal stop (idle away from any stop or depot).
- **Proof:** GPS → alert p95 under 10 s; the same alerts from clean and chaos data; no alert storm
  after a 30 min outage.

### 7. Gold with dbt: KPIs that do not change with dirty data
**Problem:** managers still had no KPIs, and the key promise was unproven: late or duplicate events
must give the same KPIs as clean data.
**Built** ([ADR 0014](docs/decisions/0014-gold-dbt-intraday-and-close.md)): a dbt star schema
(`fct_delivery`, `dim_van` as SCD Type 2, `dim_depot`, `dim_date`, `agg_depot_daily`).
- An **intraday run** every 5 minutes merges what changed in the last 2 h (provisional,
  `is_final = false`).
- A **nightly close** rebuilds the previous local day in full, marks it final and records it in
  `closed_days`. A late event after the close is counted, and applied only by an explicit restatement.
- Customer coordinates are reduced to a ~150 m area.
- dbt tests (uniqueness, relationships, KPIs recomputed by hand) and source freshness run in CI.
- **Proof:** the same day clean and with chaos gives identical gold, intraday and after the close.

![ERD: fct_delivery joined to dim_van (SCD Type 2), dim_depot and dim_date; agg_depot_daily and closed_days](docs/diagrams/model-gold.svg)

### 8. Dashboards designed, health checked
**Built:** a [dashboard design](docs/design/dashboards.md) for the dispatcher, the manager and the
operator, with the SQL of every panel runnable against the stack today
([`dashboards.sql`](docs/design/dashboards.sql)), and `make health` for the pipeline alarms
(consumer lag, checkpoints, CDC slot, bronze stuck). Building the Grafana dashboards themselves is
a stretch item ([ADR 0007](docs/decisions/0007-grafana-postgres-serving.md)).

### 9. The lakehouse on AWS, by configuration only *(in progress)*
**Problem:** the promise that the lakehouse moves to S3, Glue and Athena without code changes was
unproven, and parts of the stack leaned on Trino (table DDL, upsert keys, dbt adapter).
**Built:**
- One table definition (`infra/lakehouse/tables.py`, pyiceberg) creates every bronze, silver and
  ops table in either catalog: REST locally, Glue on AWS.
- Flink catalog settings come from one env value. Local and AWS differ only in
  `infra/local/lakehouse.env` vs `infra/aws/lakehouse.env`, and a unit test fails if code branches
  on the environment.
- dbt has an Athena target. Both engines use one strategy (merge plus a pre-hook that clears the
  days being rewritten), because dbt-athena has no `delete+insert` for Iceberg.
- Terraform (`infra/aws/lakehouse/`): an S3 bucket, Glue databases, an Athena workgroup that stops
  any query above 1 GB scanned, two least-privilege IAM roles and $5/$20 budgets.

**Proof so far:** a smoke test on AWS created the 16 resources, made all 11 tables in Glue with the
demo role, turned on Glue compaction and retention, ran INSERT, MERGE and a day DELETE through
Athena, and destroyed everything (cost under $0.01). Running the Flink jobs and dbt against AWS is
next.

<details>
<summary><b>Every reliability and scale pattern, with its trade-off and test</b></summary>

Pattern names follow *Data Engineering Design Patterns* (Konieczny).

| Pattern | Why | Trade-off | Proof (test) |
|---|---|---|---|
| Schema compatibility (FULL_TRANSITIVE) | The registry rejects a breaking schema before it reaches a running job. | A breaking change needs a new topic (`*.v2`). | `test_subjects_registered_full_transitive`, `test_v1_and_v2_pings_reach_bronze` |
| Dead-letter queue | Raw bytes and a reason are kept, so a reject can be explained and replayed. | Validation runs in Python (batched). | `test_garbage_goes_to_dlq_once_and_job_keeps_running` |
| Checkpoint + transactional commit | Iceberg commits only on a checkpoint (30 s): a restart neither loses nor doubles rows. | Bronze is up to one checkpoint behind. | `test_exactly_n_rows`, `test_taskmanager_kill_mid_load_keeps_silver_exact` |
| Resume from the newest retained checkpoint | No HA locally; a killed cluster finds its place without a savepoint. | Work since that checkpoint is redone. | `test_killed_cluster_resumes_from_retained_checkpoints` |
| Dedup + upsert, first arrival wins | Top-1 per key in state for 24 h, then an upsert for later copies. | Upserts write delete files; compaction merges them every 30 min. | `test_clean_and_chaos_runs_give_identical_silver` |
| Order by commit LSN (CDC) | Arrival order would bring a delivered order back to "out for delivery". | Postgres keeps unread WAL while Debezium is down (capped at 1 GB). | `test_order_latest_version_wins_despite_duplicates_and_disorder` |
| Late-data flag | Lateness comes from the data, so every rebuild flags the same rows. | Late rows land in an older day's partition. | `test_two_hour_late_ping_is_kept_and_marked_late` |
| State TTL | Remembering every key forever runs out of memory. | A copy after 24 h replaces the row. | `test_stack_memory_under_6gb` |
| 6 partitions keyed by van | Readers are capped by partitions; one van stays in order on one partition. | Partitions can only be added. | `test_a_van_stays_on_one_partition` |
| Time + size retention | A big load must not fill the disk: 7 days or 1 GiB per partition. | Shorter replay window under heavy load. | `test_size_bound_drops_the_oldest_segments` |
| Vectorized decode UDF | One Python call per batch instead of per message. | Still slower than a JVM decoder. | `test_batch_columns_equal_the_row_by_row_results` |
| RocksDB + incremental checkpoints | State on disk, not capped by RAM. | Slower state reads. | `test_state_lives_in_rocksdb_with_incremental_checkpoints` |
| Day partitions sorted by van | Larger files; one-van queries skip files by min/max. | Sorted only after compaction. | `test_compaction_reads_fewer_files_for_one_van_and_keeps_every_row` |
| Maintenance service | Small files, delete files and snapshots slow every read. | Up to 30 min of delete files between compactions. | `test_compaction_during_live_writes_keeps_silver_exact` |
| Keyed alert idempotency + alert horizon | One alert per order on replay, no storm after a restart. | Old events update ETAs but raise no alert. | `test_alert_latency_p95_under_10s` and the spec 006 suite |
| Commit-time intraday window + nightly close | Late changes of any age are absorbed; closed days stay fixed. | A late event after the close waits for a restatement. | spec 007 suite (AC10: clean = chaos) |

![Before and after: schema change → schema compatibility; bad message → dead-letter queue; crash mid-write → checkpoint and commit; killed cluster → resume from the newest retained checkpoint](docs/diagrams/patterns-ingest.svg)

![Before and after: a file per writer → hash distribution; 5 MB files → day partitions sorted by van; stale reader → refused resume and rebuild; piling files → maintenance service](docs/diagrams/patterns-scale-tables.svg)

![ERD: Postgres depots, vans and orders with their foreign keys; silver gps_pings, orders and order_status_history with their upsert keys and logical join keys](docs/diagrams/model-silver.svg)
</details>

## Cost
- **Local:** $0. Everything runs in Docker on a laptop (WSL2) in under 6 GB of RAM.
- **AWS:** created only on demand and destroyed after each session ([ADR 0005](docs/decisions/0005-local-first-aws-on-demand.md)).
  The AWS lakehouse has no hourly cost: S3 storage, Athena at $5 per TB scanned (each query is
  capped at 1 GB) and Glue optimizer runs. Budgets alert at $5 and $20. The smoke test cost under $0.01.

## Quick start
Prerequisites: Docker with ~6 GB free RAM, [uv](https://docs.astral.sh/uv/), and a free Docker Hub
account for the hardened MinIO image (`docker login dhi.io`, once).
```bash
make install          # Python 3.11 + dev tools
make check            # lint + unit tests (same as CI)
make up               # the whole local stack and the Flink jobs (~1 min with images cached)
make demo             # a live delivery day: orders → Postgres, GPS → Kafka
make e2e              # acceptance tests against the running stack
make down             # stop the jobs with a savepoint, then the stack (make up resumes)
```
Look inside:
```bash
make tower-status     # open orders with an ETA, open alerts, newest ping per van
make silver-status    # rows kept, duplicates removed, late rows per day
make gold-status      # per local day: orders, on-time rate, final or not
make health           # pipeline alarms: consumer lag, checkpoints, CDC slot, bronze stuck
make dlq-status       # rejected events per topic and reason
make table-health     # files, delete files, snapshots, last maintenance per table
```
Break it on purpose: `uv run python -m simulator day --chaos` (duplicates, late pings, offline
bursts), or `--invalid-rate 0.05` (bad messages → DLQ). Then compare `make silver-status` and
`make gold-status`.

More: `make silver-rebuild` (drop silver, rebuild from bronze), `make gold` / `make close DAY=…` /
`make reclose DAY=…`, `make maintain`, `make dbt-ci`, `make psql`.

UIs: Flink http://localhost:8081 · Trino http://localhost:8080 · MinIO http://localhost:9001 ·
Kafka Connect http://localhost:8083 · Schema Registry http://localhost:18081 · Postgres `localhost:15432`

**AWS lakehouse** (costs money, so destroy it after use): log in with `aws login --profile ct-admin`,
copy `infra/aws/lakehouse/terraform.tfvars.example`, then run `make aws-lake-plan` →
`make aws-lake-up` (asks to type `yes`) → `make aws-lake-down`.

## Docs
- [Intent](docs/intent.md) · [Project spec](docs/project-spec.md) · [Requirements](docs/requirements.md) · [Tech stack](docs/tech-stack.md)
- [Architecture decisions (ADRs)](docs/decisions/) · [Dashboard design](docs/design/dashboards.md)
- Diagrams are drawn with [Archify](https://github.com/tt-a1i/archify) from the JSON in [`docs/diagrams/`](docs/diagrams/).

## How this repo is built
Spec-driven: intent → spec (what and why, approved first) → plan (files, risks, rollback, proof per
criterion) → tasks, tests first → PR. Every hard-to-reverse choice gets an ADR.
See [`specs/README.md`](specs/README.md).

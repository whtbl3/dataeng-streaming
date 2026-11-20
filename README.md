# Delivery Control Tower

Real-time last-mile delivery tracking on a streaming lakehouse.
It runs locally on Docker for $0, is designed for ~100M events (ADR 0012), and
deploys to AWS managed services on demand with Terraform.

> **Status:** in development. See [roadmap](docs/roadmap.md).

## The problem
Dispatchers find out that a parcel is late only after the promised window has passed, and
managers get delivery KPIs a day late. This project predicts SLA breaches **before** they happen
and keeps KPIs correct even when GPS data arrives late or duplicated.

## Architecture
![Data flow: simulator and Postgres CDC into Kafka, Flink into Iceberg bronze and silver, dbt into gold, a Flink alert job into Postgres serving, and Trino and Grafana as consumers](docs/diagrams/data-flow.svg)

Nodes marked *built* run today (features 1–6); *planned* nodes are features 7–8 on the
[roadmap](docs/roadmap.md).

**Stream for freshness, batch for correctness:**
- Flink raises alerts within seconds.
- dbt recomputes KPIs every 5 minutes (provisional).
- A nightly job "closes the books" on the previous day (final).

See [ADR 0006](docs/decisions/0006-stream-freshness-batch-correctness.md).

**Data contracts:** every event is Avro with a registered schema (FULL_TRANSITIVE compatibility,
checked in CI too). A bad message never stops a job: it goes to `bronze.dlq_events` and
`dlq.<topic>` with the reason ([ADR 0008](docs/decisions/0008-avro-schema-registry.md),
[ADR 0010](docs/decisions/0010-dlq-raw-source-python-udf.md)).

| Layer | Local | AWS managed |
|---|---|---|
| Event log | Redpanda | Amazon MSK Serverless |
| Data contracts | Avro + Redpanda Schema Registry | AWS Glue Schema Registry |
| CDC | Debezium on Kafka Connect | MSK Connect + RDS PostgreSQL |
| Stream processing | Apache Flink | Managed Service for Apache Flink |
| Lakehouse | Iceberg on MinIO (S3 API) + REST catalog | Iceberg on S3 + Glue Data Catalog |
| SQL / transforms | Trino + dbt | Athena + dbt |
| Dashboards | Grafana + Prometheus | Grafana on ECS + CloudWatch |

## How data moves
### One GPS ping: first copy, duplicate copy and a bad message
![Sequence: a ping goes from the van to Kafka, is decoded by bronze_gps with the Schema Registry, appended to bronze, then upserted to silver by silver_gps; a duplicate is appended to bronze but produces no silver row; a ping outside the city goes to the dead-letter table and topic](docs/diagrams/gps-ping.svg)

### Order status lifecycle
![Lifecycle: created, assigned, out_for_delivery, then delivered or failed (customer absent)](docs/diagrams/order-lifecycle.svg)

## Data model
### Built: Postgres source tables and Iceberg silver
![ERD: Postgres depots, vans and orders with their foreign keys; silver gps_pings, orders and order_status_history with their upsert keys and logical join keys](docs/diagrams/model-silver.svg)

### Planned: gold star schema (feature 7)
![ERD: fct_delivery joined to dim_van (SCD Type 2), dim_depot and dim_date; agg_depot_daily and closed_days](docs/diagrams/model-gold.svg)

## Reliability and scale patterns
A stream is never complete and never clean: devices resend, tunnels delay pings, another team
changes a schema, a machine dies mid-write. So the design follows one rule:

> **Bronze keeps every raw event. Every later table can be rebuilt from bronze and gives the same
> answer**, because writes are keyed by business keys and ordered by event time or commit position,
> never by arrival.

Each pattern below fixes one way the pipeline breaks. Pattern names follow *Data Engineering Design
Patterns* (Konieczny). Read each diagram row left to right: what breaks → the pattern → what you get.

### Failures: before and after
![Before and after: schema change → schema compatibility; bad message → dead-letter queue; crash mid-write → checkpoint and commit; killed cluster → resume from the newest retained checkpoint](docs/diagrams/patterns-ingest.svg)

![Before and after: duplicate ping → dedup and upsert; replayed CDC → order by commit LSN; late ping → late-data flag; growing state → state TTL](docs/diagrams/patterns-clean.svg)

**Example: the tunnel ping.** VAN-0001 records a ping at 08:07:00 in a tunnel, sends it at 08:11:02,
and is not sure it got through, so it sends it again.

*Before (bronze keeps every copy):*

| vehicle_id | event_ts | ingest_ts |
|---|---|---|
| VAN-0001 | 08:07:00 | 08:11:02 |
| VAN-0001 | 08:07:00 | 08:11:03 |

*After (silver keeps one row per real ping, kept and flagged as late):*

| vehicle_id | event_ts | ingest_ts | lateness_s | is_late |
|---|---|---|---|---|
| VAN-0001 | 08:07:00 | 08:11:02 | 242 | true |

| Pattern | Why this design | Trade-off | Proof (test) |
|---|---|---|---|
| Schema compatibility (FULL_TRANSITIVE, checked in CI) | Producers and consumers deploy separately. The registry rejects a breaking schema before it reaches a running job. | A breaking change needs a new topic (`*.v2`). | `test_subjects_registered_full_transitive`, `test_v1_and_v2_pings_reach_bronze` |
| Dead-letter queue | One bad message must not stop 200 vans. The raw bytes and reason are kept, so a reject can be explained with SQL and replayed. | Validation runs in Python (batched, see scale). | `test_garbage_goes_to_dlq_once_and_job_keeps_running` |
| Checkpoint + transactional commit | Read position and written data must move together. Iceberg commits only on a checkpoint (every 30 s), so a restart neither loses nor doubles rows. | Bronze is up to one checkpoint behind (≤ 60 s target). | `test_exactly_n_rows`, `test_taskmanager_kill_mid_load_keeps_silver_exact` |
| Resume from the newest retained checkpoint | There is no HA locally. A killed cluster must find its place again without a savepoint. | Work since that checkpoint is redone (writes stay exactly-once). | `test_killed_cluster_resumes_from_retained_checkpoints` |
| Dedup + upsert, first arrival wins | Counting copies doubles distance. Two layers: Top-1 per `(vehicle_id, event_ts)` in state for 24 h, then an upsert on the same key for later copies ([ADR 0011](docs/decisions/0011-silver-upsert-first-arrival.md)). | Upserts write delete files; the maintenance service merges them every 30 min. | `test_clean_and_chaos_runs_give_identical_silver`, `test_resending_pings_changes_no_silver_row` |
| Order by commit LSN (CDC) | Debezium can replay or reorder changes. Arrival order would bring a delivered order back to "out for delivery". | While Debezium is down, Postgres keeps the unread WAL (capped at 1 GB). | `test_order_latest_version_wins_despite_duplicates_and_disorder`, `test_connector_restart_loses_and_duplicates_nothing` |
| Late-data flag | Dropping late pings loses data; arrival order moves vans backwards. Lateness comes from the data, so every rebuild flags the same rows. | Late rows land in an older day's partition; pings are at most about 2 h late, so only today or yesterday is touched. | `test_two_hour_late_ping_is_kept_and_marked_late` |
| State TTL | Remembering every key forever runs out of memory (≈ 100M keys in 30 days). | A copy that arrives after 24 h replaces the row (same key, newer `ingest_ts`). | `test_stack_memory_under_6gb` |

*Planned (features 6–8):* one alert per order on replay (keyed idempotency), no alert storm after
a restart (alert horizon), KPIs recomputed over a 2 h look-back, a nightly close that counts
late arrivals, audit before publish, SCD Type 2 vans, `dest_area` instead of customer coordinates,
and alerts on lag and silence.

### Scale: before and after
The design targets about 100M GPS events (30 days at 200 vans). More machines do not help when the
design itself has a ceiling, so each layer was checked for what stops it from growing
([ADR 0012](docs/decisions/0012-scale-and-table-maintenance.md)). Scaling out is now configuration:
more partitions, `GPS_PARALLELISM`, more task slots.

![Before and after: 3 partitions → 6 partitions keyed by vehicle_id; time-only retention → time and size retention; one Python call per message → vectorized UDF; heap state → RocksDB](docs/diagrams/patterns-scale-stream.svg)

![Before and after: a file per writer → hash distribution; 5 MB files → day partitions sorted by van; stale reader → refused resume and rebuild; piling files → maintenance service](docs/diagrams/patterns-scale-tables.svg)

| Change | Why this design | Trade-off | Proof (test) |
|---|---|---|---|
| 6 partitions, keyed by `vehicle_id` | Parallel readers are capped by partitions. The key keeps each van on one partition, in order. | Partitions are only ever added, never removed. | `test_gps_topic_and_its_dlq_have_6_partitions`, `test_a_van_stays_on_one_partition` |
| Time + size retention | A big load must not fill the disk: 7 days or 1 GiB per GPS partition, whichever first. | Under a very large load the replay window is shorter than 7 days. | `test_topics_are_bounded_by_size_and_time`, `test_size_bound_drops_the_oldest_segments` |
| Vectorized decode UDF | One Python call per message was the bottleneck. One call per batch: 20k → 28k msg/s. A JVM decoder or Flink's Avro format would stop on a bad message. | Still slower than a JVM decoder. | `test_batch_columns_equal_the_row_by_row_results` |
| RocksDB + incremental checkpoints | Heap state is capped by RAM. RocksDB keeps state on local disk; a checkpoint copies only what changed. | State reads are slower than on the heap. | `test_state_lives_in_rocksdb_with_incremental_checkpoints` |
| Hash write distribution | Otherwise every parallel writer writes its own small file per partition. | All rows of one partition go through one writer. | `test_each_commit_writes_one_file_per_partition` |
| Day partitions, sorted by `(vehicle_id, event_ts)` | 16 buckets a day gave ≈ 5 MB files (target 128–512 MB). Sorting lets a one-van query skip files by min/max. | Files are sorted only after compaction. | `test_silver_gps_is_one_partition_a_day_sorted_by_van`, `test_compaction_reads_fewer_files_for_one_van_and_keeps_every_row` |
| Refuse a stale resume | After 7 days a reader's snapshot is expired, and Iceberg would fail later with an unclear error. | A reader stopped longer than 7 days needs `make silver-rebuild`. | `test_reader_stopped_past_snapshot_retention_is_refused_and_rebuild_recovers` |
| Maintenance service | Small files, delete files, manifests and snapshots pile up and slow every read. It compacts silver every 30 min, bronze every hour, and daily expires snapshots, removes orphans and drops partitions past retention. | Between compactions, reads apply up to 30 min of delete files. | `test_compaction_during_live_writes_keeps_silver_exact`, `test_expiry_and_orphan_removal`, `test_retention_drops_partitions_past_their_age_and_jobs_keep_running` |

Throughput at 100M events is reasoned from these limits, not measured: the benchmark is a stretch item.

The diagrams are drawn with [Archify](https://github.com/tt-a1i/archify) from the JSON files in
[`docs/diagrams/`](docs/diagrams/), which also explains how to regenerate them.

## Quick start
Prerequisites: Docker with ~6 GB free RAM, [uv](https://docs.astral.sh/uv/), and a free
Docker Hub account for the hardened MinIO image (`docker login dhi.io`, once).
```bash
make install                  # Python 3.11 + dev tools
make check                    # lint + unit tests (same as CI)
make up                       # core + CDC stack, Flink jobs (~1 min with images cached)
make demo                     # live delivery day, 20 vans: orders → Postgres, GPS → Kafka
make cdc-status               # Debezium connector state + replication slot lag
make dlq-status               # rejected events per source topic and reason
make silver-status            # silver: rows kept, duplicate copies removed, late rows per day
make silver-rebuild           # drop silver and rebuild it from bronze (same result)
make down                     # stop jobs with a savepoint, then the stack (make up resumes)
make e2e                      # acceptance tests against the running stack
make maintain                 # table maintenance now: compaction, manifests, expiry, orphans, retention
make table-health             # files, delete files, file size, snapshots and last maintenance per table
make tower-status             # control tower: open orders with an ETA, open alerts, newest ping per van
```
Query the bronze table:
```bash
docker compose -f infra/local/docker-compose.yml exec trino trino --execute \
  "SELECT count(*), max(event_ts) FROM lakehouse.bronze.gps_pings"
# every change of one order, in commit order (CDC changelog)
docker compose -f infra/local/docker-compose.yml exec trino trino --execute \
  "SELECT op, before.status, after.status, commit_ts FROM lakehouse.bronze.orders_cdc
   WHERE after.order_id = 'ORD-20261118-00001' ORDER BY source_lsn"
```
Try the dead-letter path: `uv run python -m simulator day --invalid-rate 0.05`, then `make dlq-status`.
Try dirty data: `uv run python -m simulator day --chaos` sends duplicates, pings up to 2 h late,
jitter and offline bursts; `make silver-status` shows bronze has more rows than silver, which
keeps one row per real ping (spec 004).

UIs: Flink http://localhost:8081 · Trino http://localhost:8080 · MinIO http://localhost:9001 · Kafka Connect http://localhost:8083 · Schema Registry http://localhost:18081 · Postgres `localhost:15432` (`make psql`)

## Docs
- [Intent](docs/intent.md) · [Project spec](docs/project-spec.md) · [Tech stack](docs/tech-stack.md)
- [Architecture decisions (ADRs)](docs/decisions/)
- Feature specs: [`specs/`](specs/)

## How this repo is built
The project follows a spec-driven workflow: intent → spec → plan → tasks → PR.
See [`specs/README.md`](specs/README.md).

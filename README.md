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

Nodes marked *built* run today (features 1–5); *planned* nodes are features 6–8 on the
[roadmap](docs/roadmap.md). More diagrams (click to open):

<details>
<summary><b>One GPS ping</b>: first copy, duplicate copy and a bad message</summary>

![Sequence: a ping goes from the van to Kafka, is decoded by bronze_gps with the Schema Registry, appended to bronze, then upserted to silver by silver_gps; a duplicate is appended to bronze but produces no silver row; a ping outside the city goes to the dead-letter table and topic](docs/diagrams/gps-ping.svg)
</details>

<details>
<summary><b>Order status lifecycle</b>: created → assigned → out_for_delivery → delivered or failed</summary>

![Lifecycle: created, assigned, out_for_delivery, then delivered or failed (customer absent)](docs/diagrams/order-lifecycle.svg)
</details>

<details>
<summary><b>Data model, built</b>: Postgres source tables and Iceberg silver</summary>

![ERD: Postgres depots, vans and orders with their foreign keys; silver gps_pings, orders and order_status_history with their upsert keys and logical join keys](docs/diagrams/model-silver.svg)
</details>

<details>
<summary><b>Data model, planned</b>: gold star schema (feature 7)</summary>

![ERD: fct_delivery joined to dim_van (SCD Type 2), dim_depot and dim_date; agg_depot_daily and closed_days](docs/diagrams/model-gold.svg)
</details>

The diagrams are drawn with [Archify](https://github.com/tt-a1i/archify) from the JSON files in
[`docs/diagrams/`](docs/diagrams/), which also explains how to regenerate them.

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

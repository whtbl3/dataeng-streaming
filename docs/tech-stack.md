# Tech stack

| Area | Choice (local) | AWS managed equivalent | Why | Alternatives considered |
|---|---|---|---|---|
| Language | Python 3.11 (uv) | — | PyFlink supports up to 3.11; one language across simulator, jobs and tests | Java/Scala Flink (more boilerplate) |
| Message bus | Redpanda (Kafka API) | Amazon MSK Serverless | Most transferable skill; same client code everywhere (ADR 0001) | Kinesis Data Streams |
| Schemas | Avro + Redpanda Schema Registry (FULL_TRANSITIVE), `fastavro` 1.13.1, Confluent Avro converter 8.4.0 for Debezium | AWS Glue Schema Registry | Data contracts, compatibility checks in the registry and in CI (ADR 0008) | JSON + JSON Schema in CI |
| CDC | Postgres 16 + Debezium (Kafka Connect) | RDS PostgreSQL + MSK Connect | Realistic OLTP → stream pattern (ADR 0004) | DMS; app writes to Kafka directly |
| Stream processing | Apache Flink 1.20 (Flink SQL + PyFlink 1.20.5 UDFs for decode/validation, ADR 0010) | Managed Service for Apache Flink | Event time, watermarks, state, exactly-once (ADR 0002) | Spark Structured Streaming; plain consumers |
| Object storage | MinIO `RELEASE.2025-09-07` via Docker Hardened Images (`dhi.io`) | Amazon S3 | S3-compatible API; upstream stopped publishing images in 2025, DHI is maintained by Docker | SeaweedFS, RustFS, Bitnami legacy image |
| Table format | Apache Iceberg | Iceberg on S3 | Native in Athena, Flink, Trino; ACID + time travel (ADR 0003) | Delta Lake, Hudi |
| Catalog | Iceberg REST catalog | AWS Glue Data Catalog | Swap by config only | Nessie, Hive Metastore |
| Query engine | Trino | Amazon Athena | Same Iceberg tables, ANSI SQL | DuckDB, Spark SQL |
| Table definitions | pyiceberg 0.12 (`glue`, `pyarrow`): one definition for both catalogs (ADR 0015) | same, against Glue | No Trino DDL; upsert keys and properties in one place | Trino DDL + an Athena copy |
| Transformations | dbt-core 1.11 + dbt-trino 1.11 | dbt-athena 1.11 | Tested, documented gold models | Flink SQL only; Spark |
| Scheduling | `dbt` container: intraday every 5 min + nightly close | ECS Fargate tasks + EventBridge Scheduler (2 schedules) | Two simple schedules do not need Airflow | Airflow / MWAA, Dagster |
| Serving store | Postgres `serving` schema | same RDS instance | Low-latency upserts for live panels (ADR 0007) | Redis, DynamoDB |
| Dashboards | Grafana OSS (provisioned as code) | Grafana OSS on ECS Fargate | Real-time panels + Geomap + ops metrics in one tool | Streamlit, Superset, QuickSight |
| Metrics | Prometheus | Amazon CloudWatch | Consumer lag, checkpoints, throughput | — |
| IaC | Docker Compose | Terraform 1.9 (pinned image if not installed); lakehouse root `infra/aws/lakehouse/` | Reproducible, one-command up/down (ADR 0005) | CDK, CloudFormation |
| Runtime host | Laptop, WSL2 10 GB / 8 vCPU, compose profiles | — | $0 daily iteration (ADR 0005) | EC2 devbox (fallback) |
| Tests | pytest, PyFlink MiniCluster, dbt tests + unit tests | — | Logic, job and data-quality layers | Great Expectations |
| Lint / format | ruff, sqlfluff (dbt), terraform fmt/validate | — | Fast, one config each | black + flake8 |
| CI | GitHub Actions | — | `check` on every PR; `e2e` job on demand | — |

## Conventions
```
simulator/          Python package: fleet + order generators (HCMC bounding box)
flink/jobs/         Flink SQL jobs (*.sql) + PyFlink entrypoints
flink/udf/          Python UDFs (e.g. haversine, ETA) — pure functions, unit tested
schemas/            Avro schemas = data contracts: schemas/<subject>/v1.avsc, v2.avsc, ...
contracts/          wire format, registry client, rules, decoders, PyFlink UDFs (spec 003)
dbt/                dbt project: staging (silver) → marts (gold)
infra/local/        docker-compose.yml + service configs
infra/lakehouse/    the one table definition (tables.py) + lakehouse-init image
infra/aws/          Terraform: lakehouse/ (S3, Glue, Athena, IAM, budgets); the managed env later
bench/              backfill + benchmark harness, results in bench/results/*.md
grafana/            provisioned datasources + dashboards (JSON)
tests/unit/         fast tests, no Docker
tests/integration/  needs `make up`; run by `make e2e`
```
- Topic names: `<domain>.<entity>.v<N>`; Iceberg tables: `<layer>.<entity>`.
- All timestamps are UTC `timestamp(3)`. Event time is always `event_ts`, ingest time is `ingest_ts`.
- Environments: `local` (everything) and `managed` (demos). Their differences live **only**
  in env/config files, never in code.
- Every AWS resource is tagged `project=delivery-control-tower`, which `make aws-down` and the budget alert use.

## Local S3
- Default: `dhi.io/minio:RELEASE.2025-09-07T16-13-09Z-debian13` (non-root, no shell). It needs
  `docker login dhi.io` with a free Docker Hub account. Bucket creation is done by a one-shot
  `amazon/aws-cli` container, which also acts as MinIO's readiness gate.
- Without a Docker account, replace the `minio` service in `infra/local/docker-compose.yml`
  (image, command, credentials) with another S3-compatible server such as
  `chrislusf/seaweedfs` (`server -s3`). Nothing else changes: every client only sees an S3
  endpoint plus keys.

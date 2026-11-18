# Delivery Control Tower

Real-time last-mile delivery tracking on a streaming lakehouse.
It runs locally on Docker for $0, is designed and benchmarked for ~100M events, and
deploys to AWS managed services on demand with Terraform.

> **Status:** in development. See [roadmap](docs/roadmap.md).

## The problem
Dispatchers find out that a parcel is late only after the promised window has passed, and
managers get delivery KPIs a day late. This project predicts SLA breaches **before** they happen
and keeps KPIs correct even when GPS data arrives late or duplicated.

## Architecture
```
Simulator (HCMC, 200 vans) ── GPS ───────────────> Kafka ──> Flink ──> Iceberg bronze/silver ─dbt─> gold
          └── orders ─> Postgres ─ Debezium CDC ─┘           └──> Postgres serving (ETA, alerts) ──> Grafana
```
**Stream for freshness, batch for correctness:**
- Flink raises alerts within seconds.
- dbt recomputes KPIs every 5 minutes (provisional).
- A nightly job "closes the books" on the previous day (final).

See [ADR 0006](docs/decisions/0006-stream-freshness-batch-correctness.md).

| Layer | Local | AWS managed |
|---|---|---|
| Event log | Redpanda | Amazon MSK Serverless |
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
make up                       # core stack + Flink job (~30 s with images cached)
make demo                     # live simulator, 20 vans (Ctrl-C to stop)
make e2e                      # acceptance tests against the running stack
make backfill DAYS=30         # ~100M events      (roadmap feature 5)
make bench                    # throughput report (roadmap feature 5)
```
Query the bronze table:
```bash
docker compose -f infra/local/docker-compose.yml exec trino trino --execute \
  "SELECT count(*), max(event_ts) FROM lakehouse.bronze.gps_pings"
```
UIs: Flink http://localhost:8081 · Trino http://localhost:8080 · MinIO http://localhost:9001

## Docs
- [Intent](docs/intent.md) · [Project spec](docs/project-spec.md) · [Tech stack](docs/tech-stack.md)
- [Architecture decisions (ADRs)](docs/decisions/)
- Feature specs: [`specs/`](specs/)

## How this repo is built
The project follows a spec-driven workflow with Claude Code: intent → spec → plan → tasks → PR.
See `CLAUDE.md`, `.claude/` and `specs/README.md`.

# Roadmap

Target: 6 weeks, part-time. Each row becomes `specs/NNN-*/` via `/new-feature`.
Items marked *(cut if late)* can move to "Stretch" without breaking the story.

| # | Week | Feature | Status | Spec |
|---|---|---|---|---|
| 1 | 1 | Walking skeleton: simulator → Redpanda `gps.pings.v1` → Flink → Iceberg bronze on MinIO → query in Trino; one test; `make up` / `make check`; CI | Implemented | [001](../specs/001-walking-skeleton/spec.md) |
| 2 | 2 | Order domain: Postgres schema + seed (depots, vans), order simulator, Debezium CDC → Kafka → bronze | Planned | — |
| 3 | 2 | Data contracts: Avro schemas + Schema Registry (BACKWARD), invalid events → DLQ | Planned | — |
| 4 | 3 | Silver: dedup + event-time watermarks; simulator chaos mode (late, out-of-order, duplicate events); tests | Planned | — |
| 5 | 3 | Scale: data tiers (`dev` / `standard` 100M / `stress`), `make backfill DAYS=N`, Iceberg partitioning + compaction, bounded Kafka retention, `make bench` → `bench/results/` | Planned | — |
| 6 | 3–4 | Control tower job: latest position ⋈ active orders → ETA, SLA-risk + abnormal-stop alerts → Postgres serving + `alerts.v1` | Planned | — |
| 7 | 4 | Gold with dbt: `fct_delivery`, `agg_depot_daily`, ETA error; dbt tests + source freshness; intraday run (5 min, 2 h look-back) + nightly reconciliation (close previous day, `is_final`, `closed_days`) | Planned | — |
| 8 | 4 | Grafana: live map, alerts, KPI (provisional vs final) and pipeline-health dashboards (Prometheus) | Planned | — |
| 9 | 5 | AWS-native lakehouse (local Flink/dbt → real AWS): MinIO/REST/Trino → S3 + Glue Catalog + Athena by config only; dbt-athena | Planned | — |
| 10 | 5 | Managed deploy: Terraform (MSK Serverless, MSK Connect, Managed Flink, RDS, ECS); `make aws-up` / `aws-down` *(cut if late: keep the Terraform + one recorded demo)* | Planned | — |
| 11 | 6 | Hardening and story: replay from offsets, `stress` run, README (architecture, trade-offs, benchmark, cost), demo video | Planned | — |

**Stretch (after v1):** ML-based ETA, EC2 devbox for repeatable benchmarks, Kinesis vs MSK cost/latency comparison, OpenLineage.

Status: Planned · In spec · In progress · Implemented

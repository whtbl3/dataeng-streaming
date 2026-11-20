# Roadmap

Target: 6 weeks, part-time. Each row becomes `specs/NNN-*/`.
Items marked *(cut if late)* can move to "Stretch" without breaking the story.

| # | Week | Feature | Status | Spec |
|---|---|---|---|---|
| 1 | 1 | Walking skeleton: simulator → Redpanda `gps.pings.v1` → Flink → Iceberg bronze on MinIO → query in Trino; one test; `make up` / `make check`; CI | Implemented | [001](../specs/001-walking-skeleton/spec.md) |
| 2 | 2 | Order domain: Postgres schema + seed (depots, vans), order simulator, Debezium CDC → Kafka → bronze | Implemented | [002](../specs/002-orders-cdc/spec.md) |
| 3 | 2 | Data contracts: Avro schemas + Schema Registry (FULL_TRANSITIVE), invalid events → DLQ | Implemented | [003](../specs/003-data-contracts/spec.md) |
| 4 | 3 | Silver: dedup + event-time watermarks; simulator chaos mode (late, out-of-order, duplicate events); tests | Implemented | [004](../specs/004-silver/spec.md) |
| 5 | 3 | Scale: more GPS partitions, batched decode, state on disk, Iceberg layout + compaction + expiry, bounded Kafka retention + table maintenance service | Implemented | [005](../specs/005-scale/spec.md) |
| 6 | 3–4 | Control tower job: latest position ⋈ active orders → ETA, SLA-risk + abnormal-stop alerts → Postgres serving + `alerts.v1` | Implemented | [006](../specs/006-control-tower/spec.md) |
| 7 | 4 | Gold with dbt: `fct_delivery`, `agg_depot_daily`, ETA error; dbt tests + source freshness; intraday run (5 min, 2 h look-back) + nightly reconciliation (close previous day, `is_final`, `closed_days`) | Implemented | [007](../specs/007-gold/spec.md) |
| 8 | 4 | Grafana: live map, alerts, KPI (provisional vs final) and pipeline-health dashboards (Prometheus) | Planned | — |
| 9 | 5 | AWS-native lakehouse (local Flink/dbt → real AWS): MinIO/REST/Trino → S3 + Glue Catalog + Athena by config only; dbt-athena | Planned | — |
| 10 | 5 | Managed deploy: Terraform (MSK Serverless, MSK Connect, Managed Flink, RDS, ECS); `make aws-up` / `aws-down` *(cut if late: keep the Terraform + one recorded demo)* | Planned | — |
| 11 | 6 | Hardening and story: replay from offsets, `stress` run, README (architecture, trade-offs, benchmark, cost), demo video | Planned | — |

**Stretch (after v1):** data tiers + `make backfill` + `make bench` (dropped from feature 5, ADR 0012), ML-based ETA, EC2 devbox for repeatable benchmarks, Kinesis vs MSK cost/latency comparison, OpenLineage.

Status: Planned · In spec · In progress · Implemented

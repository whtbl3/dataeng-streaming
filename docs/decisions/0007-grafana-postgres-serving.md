# 0007: Grafana dashboards backed by a Postgres serving layer
Date: 2026-11-17 · Status: Final

## Context
Dispatchers need a live map and alert list (second-level freshness). Managers need KPI
charts from the lakehouse. Operators need pipeline health.

## Decision
Flink upserts `vehicle_latest`, `order_eta` and `alerts` into the Postgres `serving` schema.
Grafana reads Postgres (live panels, Geomap), Trino/Athena (gold KPIs) and Prometheus/CloudWatch
(lag, checkpoints). Dashboards are provisioned as JSON in git.

## Options considered
- **Grafana (chosen):** one tool for ops, BI-lite and observability; dashboards as code.
- **Streamlit:** flexible UI, but observability must be built separately.
- **Superset/QuickSight:** BI-oriented and not suited to second-level data; QuickSight adds per-user cost.

## Consequences
+ Real-time panels never query the lakehouse, which keeps Athena cost near zero.
− Postgres is shared with the OLTP orders schema (acceptable for a demo; split in production).

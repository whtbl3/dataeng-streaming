# 0003: Apache Iceberg lakehouse — REST catalog + Trino locally, Glue + Athena on AWS
Date: 2026-11-17 · Status: Final

## Context
We need an open table format on object storage with ACID upserts (dedup/merge),
schema evolution, and time travel, readable from Flink, dbt and a SQL engine.

## Decision
Use Apache Iceberg with medallion namespaces `bronze` / `silver` / `gold`.
- Locally: MinIO, the Iceberg REST catalog and Trino.
- On AWS: S3, the Glue Data Catalog and Athena.
Only the catalog configuration differs.

## Options considered
- **Iceberg (chosen):** first-class in Athena, Glue, Flink and Trino, and AWS's strategic format (S3 Tables).
- **Delta Lake:** strong in Databricks; Athena support is weaker for writes.
- **Hudi:** good for upserts, but more operational complexity and less momentum.

## Consequences
+ Time travel gives "KPIs as of" queries and easy debugging of late data.
− Small files from streaming writes need compaction (scheduled `optimize`/rewrite).
− dbt adapters differ (dbt-trino vs dbt-athena), so models must stay in portable SQL.

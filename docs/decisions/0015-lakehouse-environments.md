# 0015: One lakehouse, two environments: switched by configuration only
Date: 2026-11-20 · Status: Final (lakehouse layer); the Flink and dbt runs on AWS follow in feature 10

## Context
ADR 0003 and 0005 promise that the lakehouse moves from MinIO, the Iceberg REST catalog and Trino
to S3, the Glue Data Catalog and Athena without code changes. Before spec 009 several parts leaned
on the local engines:
- Flink jobs wrote the REST catalog settings inline;
- tables were created with Trino DDL, and the upsert keys and `write.*` properties were set by
  raw REST calls, because Trino cannot set them;
- dbt had only the Trino adapter, and the nightly close used `delete+insert`, which dbt-athena
  does not offer for Iceberg.

The AWS side must also stay cheap and safe for a solo owner with ~$190 of credit: nothing runs
24/7, nothing is created without an explicit confirmation, and no credential is stored.

## Decision
- **Catalog settings are configuration.** One env value, `ICEBERG_CATALOG_OPTIONS`, holds the Flink
  catalog options (REST + MinIO locally, `GlueCatalog` + S3 on AWS). The env file is
  `infra/local/lakehouse.env` (committed) or `infra/aws/lakehouse.env` (gitignored, copied from
  the `.example` with the Terraform outputs). `make up LAKEHOUSE=aws` picks the second one and
  leaves the local lakehouse services (compose profile `lake`) off.
- **One table definition** in Python (`infra/lakehouse/tables.py`, pyiceberg 0.12 with the `glue`
  and `pyarrow` extras) holds the schema, partitioning, sort order, upsert keys and properties of
  every bronze, silver and ops table. A one-shot `lakehouse-init` service creates missing tables in
  whichever catalog is configured and checks existing ones (a difference fails, nothing evolves
  silently). Trino DDL and the REST patching script are gone.
- **dbt uses one strategy on both engines:** `merge` on the business key, with a pre-hook that
  deletes the days a run rewrites (the closing day, or the open days for the daily aggregate).
  Partition config is declared for both adapters (`properties` for Trino, `partitioned_by` for
  Athena); the models do not branch on the adapter.
- **Maintenance on AWS** uses the Glue table optimizers (compaction, snapshot retention, orphan
  deletion), turned on by `lakehouse-init` when `GLUE_OPTIMIZER_ROLE_ARN` is set. Locally the
  maintenance service keeps doing the same work through Trino (ADR 0012).
- **AWS resources** live in their own Terraform root, `infra/aws/lakehouse/`: an S3 bucket
  (`force_destroy`), Glue databases, an Athena workgroup that stops any query over 1 GB scanned,
  a demo role scoped to this bucket, these databases and the workgroup, a Glue optimizer role, and
  budgets at $5 and $20. Region `ap-southeast-1`. `make aws-lake-up` prints what it creates and
  asks to type `yes`; `make aws-lake-down` removes everything.
- **Credentials:** the owner logs in with `aws login` (console sign-in of an IAM user, short-lived
  credentials). Make recipes export them as environment variables for one command only; the jobs
  use the demo role. IAM Identity Center was not used: it needs an AWS Organization, which ends the
  account's free-plan credits.

## Options considered
- **Config-only switch with one table definition (chosen):** one source of truth; needs pyiceberg
  as a small extra service.
- **Trino DDL plus an Athena copy:** no new library, but two files per table to keep in step, and
  Athena's Iceberg DDL cannot set upsert keys either.
- **Run Trino on AWS over Glue:** the same SQL everywhere, but a server running 24/7 and no proof
  that Athena works.
- **A local AWS emulator:** free, but it runs Athena on SQLite and cannot read Glue or Iceberg
  tables, so it proves nothing about this lakehouse (spec 009).
- **`insert_overwrite` with a staging table for the close on Athena:** more moving parts than
  merge plus a day-clearing pre-hook.

## Consequences
- A smoke test on AWS (2026-11-20) created the 16 resources, made all 11 tables in Glue with the
  demo role, turned on the optimizers, ran INSERT, MERGE on the upsert key and a day DELETE in
  Athena, and destroyed everything (cost under $0.01). Three gaps showed only on real AWS and were
  fixed in configuration: pyiceberg needs a FileIO to write metadata with Glue, boto3 reads
  `AWS_DEFAULT_REGION`, and compose must not override the region.
- Not yet run on AWS: the Flink jobs against Glue, `dbt build --target aws` (time zone and Bing
  tile functions on Athena), Athena-side partition retention, and a measured demo cost. They are
  part of the full demo in feature 10.
- Adding a table means one entry in `tables.py`; changing an existing table's shape needs an
  explicit migration (the check refuses it).
- Reverse: `make aws-lake-down`, then revert the branch; the local stack keeps the same tables.

# Plan 009: AWS lakehouse (S3, Glue, Athena by configuration)
From: spec.md (Approved 2026-11-20) · Approved by: project owner (2026-11-20)
## Context
Spec `specs/009-aws-lakehouse/spec.md` (Approved 2026-11-20, 8 ACs, 7 resolved questions; no
emulator). ADR 0003/0005 promise that the lakehouse moves from MinIO + Iceberg REST + Trino to
S3 + Glue + Athena by configuration only. Reading the code shows what is engine- or
catalog-specific today:
- Flink catalog settings are written inline as REST (`flink/jobs/_init_catalog.sql`,
  `silver_gps.sql` source, `control_tower.py`).
- Tables are created with Trino DDL (`infra/local/trino/init/001-006`), and identifier fields and
  `write.*` properties are set through raw REST calls (`infra/local/iceberg/table_admin.py`).
- Maintenance, the gold scheduler and the status commands talk to Trino.
- dbt has only the Trino adapter.
Nothing is created on AWS without the owner's explicit OK in that session.

## Spike outcome (T1, 2026-11-20)
- **S1 one table definition works.** `infra/lakehouse/tables.py` (pyiceberg 0.12.0) checked against
  the 11 tables Trino created: no difference in columns, types, required flags, partitions, sort
  order, identifier fields or `write.*` properties; a deliberate change (month instead of day) is
  reported. Tables it creates in the REST catalog read and write in Trino exactly like the Trino
  ones (`format_version = 2`, `NOT NULL` keys, nested ROW types).
- **S2 Flink has what Glue needs:** `GlueCatalog` is in `iceberg-flink-runtime-1.20-1.10.1.jar`, the
  Glue and S3 SDK clients in `iceberg-aws-bundle-1.10.1.jar`; no new jar.
- **S3 dbt-athena 1.11.1 has only `append` and `merge` for Iceberg** (no `delete+insert`). So both
  engines now use one path: `merge`, with a pre-hook that deletes the partitions being rewritten
  (the closing day, or the open days of the aggregate). The partition config is declared for both
  adapters at once (`properties` is read by dbt-trino, `partitioned_by` and `table_type` by
  dbt-athena; each ignores the other's keys), so no macro branches on the adapter either.
  Cost: the delete and the merge are two commits; a failure between them leaves the day short
  until the next run rewrites it (the next intraday run, or the close run again).
- **S4** (`terraform validate`) is done with T5.

## Design
**1. Catalog settings from one env value.**
- `ICEBERG_CATALOG_OPTIONS` holds the Flink catalog options as `'k' = 'v'` pairs. It is used by
  `_init_catalog.sql`, the `silver_gps` source (plus its `catalog-database`/`catalog-table`) and
  `control_tower.py`.
- `infra/local/lakehouse.env` (committed, local values: REST URI, MinIO endpoint, path-style,
  the local keys already in compose).
- `infra/aws/lakehouse.env.example` (Glue: `catalog-impl`, `org.apache.iceberg.aws.glue.GlueCatalog`,
  `warehouse` = `s3://<bucket>/`, `client.region`; credentials from the default chain, no keys).
- `make up LAKEHOUSE=aws` passes `--env-file infra/aws/lakehouse.env` (the owner copies it from the
  example; it is gitignored).

**2. One table definition** (`infra/lakehouse/tables.py`, pyiceberg, pinned):
- Every bronze, silver and ops table: schema, partition spec, sort order, identifier fields and
  `write.*` properties, which today are split over Trino SQL and `table_admin.py`.
- `create_missing()` creates them in whichever catalog pyiceberg is configured for:
  `PYICEBERG_CATALOG__LAKEHOUSE__*` env, REST locally, Glue on AWS.
- Existing tables are checked, not evolved: a schema or partition difference fails loudly.
- It replaces `trino/init/*.sql` (gone), `table_admin.py` (folded in) and `trino-init`, through a
  `lakehouse-init` one-shot service.
- In AWS mode it also enables the Glue table optimizers (compaction, snapshot retention, orphan
  deletion) through boto3, driven by config.

**3. Compose modes.**
- The local lakehouse services (minio, minio-init, iceberg-rest, trino, maintenance) move to a
  `lake` profile, on by default (`PROFILES ?= lake cdc`).
- AWS mode runs `PROFILES=cdc` with the AWS env file. Short-lived credentials come from
  `aws configure export-credentials --profile $(AWS_PROFILE) --format env`, inside the make
  recipe, and are passed to jobmanager, taskmanager, flink-submit, dbt and lakehouse-init as
  environment only; no file is written.

**4. dbt on Athena.**
- `profiles.yml.example` gets an `aws` target (type athena, `awsdatacatalog`, workgroup, S3
  staging dir, region, all `env_var`).
- The dbt image adds dbt-athena, pinned.
- Engine differences stay in macros:
  - `partitioned_by()` per adapter;
  - (S3) one strategy for both engines: `merge` plus a pre-hook `DELETE` of the partitions
    being rewritten, because dbt-athena has no `delete+insert` for Iceberg.
- Models are unchanged.

**5. Terraform** (`infra/aws/lakehouse/`, its own root; `aws-plan/up/down` stay for feature 10):
- S3 bucket (`force_destroy`, versioning off, project tags).
- Glue databases `bronze`, `silver`, `gold`, `gold_staging`, `ops`.
- Athena workgroup with a 1 GB scan cutoff and its results prefix.
- A demo IAM role, assumable by the owner's identity, scoped to the bucket, those databases and
  the workgroup.
- A Glue optimizer role.
- AWS Budgets at $5 and $20. The notification email is a variable in `terraform.tfvars.example`,
  filled in by the owner and never committed.
- Make targets:
  - `aws-lake-plan`;
  - `aws-lake-up`: prints the cost estimate and asks to type `yes`;
  - `aws-lake-down`.

**6. Demo and proof on AWS** (`scripts/aws_demo.py`, boto3, only after the owner's OK):
- Simulate an hour, count bronze/silver in Athena against what was sent, and build gold.
- Compare KPIs with the same seed run locally.
- Read the cost from Cost Explorer by the project tag a day later (Cost Explorer lags) and record
  it in `docs/cost.md`.

**7. AC1 check:** a unit test fails if a job, model, macro outside the adapter macros, or script
branches on the environment name (patterns `LAKEHOUSE`, `== "aws"`, `athena` outside the allowed
files).

## Files that change
- **New:**
  - `infra/lakehouse/tables.py` (+ `tests/unit/test_tables.py`);
  - `infra/local/lakehouse.env`, `infra/aws/lakehouse.env.example`;
  - `infra/aws/lakehouse/*.tf` + `terraform.tfvars.example`;
  - `scripts/aws_demo.py`;
  - `docs/decisions/0015-lakehouse-environments.md`, `docs/cost.md`;
  - `tests/unit/test_no_env_branching.py`;
  - `specs/009-aws-lakehouse/{plan,tasks}.md`.
- **Changed:**
  - `flink/jobs/{_init_catalog.sql,silver_gps.sql,control_tower.py}`;
  - `infra/local/docker-compose.yml`;
  - `infra/local/flink/submit.sh` (render the options);
  - `infra/local/dbt/Dockerfile`, `dbt/profiles.yml.example`, `dbt/macros/*`,
    `dbt/models/marts/fct_delivery.sql`, `agg_depot_daily.sql` (strategy macro only);
  - `infra/local/silver_rebuild.sh` (recreate via `tables.py`);
  - `Makefile`, `.gitignore`, `pyproject.toml` (pyiceberg, dbt-athena);
  - tests that read the Trino init SQL.
- **Removed:** `infra/local/trino/init/*.sql`, `infra/local/iceberg/table_admin.py` (+ its test, folded).

## Order of work
1. **Spikes, local and free:**
   - S1: pyiceberg recreates every table identically to today. Compare REST metadata: schema,
     spec, sort, identifier fields, properties.
   - S2: Flink starts with the options from env, local REST unchanged; the bundled jar has
     `GlueCatalog`.
   - S3: dbt-athena compiles the models, and the close strategy works on paper (adapter docs).
   - S4: `terraform validate`.
2. Catalog options from env; local e2e unchanged (regression).
3. `tables.py` replaces the Trino DDL and `table_admin.py`; local e2e (AC3 locally).
4. dbt aws target and macros; `make dbt-ci` stays green.
5. Terraform, make targets with confirmation; `terraform plan` once the owner has logged in
   (free, no resources).
6. **Owner's OK → AWS demo:** `aws-lake-up`, demo, checks (AC2, AC4, AC5, AC6), `aws-lake-down`;
   cost (AC7).
7. ADR 0015, docs; `make check`, `make e2e`; spec Implemented; PR.

## Risks and rollback
- **R1 pyiceberg vs Trino-created tables** (S1): small type or property differences (for example
  the `format` or field ids). New installs use the definition; existing local tables are only
  checked.
- **R2 dbt-athena Iceberg strategies** (S3): if MERGE with a delete pre-hook is not enough, the close
  writes through `insert_overwrite` on a staging table plus a swap. Decided in S3.
- **R3 credentials expire** (short-lived `aws login` session): the demo fits in an hour; otherwise log in again
  and restart the services.
- **R4 cost overrun:** the 1 GB scan cutoff, budgets, `force_destroy` and `aws-lake-down` straight
  after the demo; nothing runs 24/7 on AWS.
- **Rollback:** `make aws-lake-down`; revert the branch (the local stack keeps the same tables,
  now created by `tables.py`).

## Proof
| AC | Test |
|---|---|
| AC1 same files | unit `test_no_env_branching.py`; review: only env/config files differ |
| AC2 infra up/down | `terraform plan`/`apply`/`destroy` output; `aws s3 ls` / Glue `get-databases` empty after down |
| AC3 tables from one definition | unit `test_tables.py`; local e2e (REST metadata matches); AWS: Glue tables listed by the demo |
| AC4 bronze/silver on Athena | `aws_demo.py` counts vs sent / deduplicated |
| AC5 gold on Athena = local | `aws_demo.py` runs `dbt build --target aws` and compares KPIs with the local run |
| AC6 maintenance | Glue optimizer status read by the demo; retention task through Athena |
| AC7 cost < $1 | Cost Explorer by tag, recorded in `docs/cost.md` |
| AC8 no stored credentials | review + unit check that the env files hold no key names with values; make targets prompt before create |

## Verification
- Local: `make check`, `make dbt-ci`, `make clean && make up`, `make e2e` (the same results with
  tables created by `tables.py`).
- AWS, only with the owner's OK:
  1. `aws login --profile ct-admin` (console sign-in of an IAM user; IAM Identity Center would need an AWS Organization, which ends the free-plan credits);
  2. `make aws-lake-plan`, then `make aws-lake-up` (typing `yes`);
  3. `make up LAKEHOUSE=aws`;
  4. `uv run python scripts/aws_demo.py`;
  5. `make aws-lake-down`;
  6. `aws s3 ls`, to check nothing is left.

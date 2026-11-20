# Spec 009: AWS lakehouse (S3, Glue, Athena by configuration)
Status: Draft · Intent: roadmap item 9 · Author: project owner

<!-- WHAT and WHY only. No file names, no code, no SQL. -->

## Problem
The intent asks that each local component map to a named AWS managed service, and ADR 0003 and
0005 promise that the lakehouse moves from MinIO, the REST catalog and Trino to S3, the Glue Data
Catalog and Athena by configuration alone. Nothing proves it yet. Some parts of the local stack
also lean on Trino in ways Athena may not accept: the tables are created with Trino DDL, the
maintenance runs Trino procedures, and dbt uses the Trino adapter.

## Outcome
- One switch of configuration points the existing Flink jobs, the table setup, dbt and the
  maintenance at S3 + Glue + Athena; no job, model or script has an AWS branch in its code.
- The AWS lakehouse resources are created and removed by one command each, with a cost guard,
  and nothing is created without the owner's explicit go-ahead in that session.
- A short demo proves it end to end: local Kafka and Flink write bronze and silver to S3/Glue,
  dbt builds gold on Athena, and the KPIs equal the local ones for the same simulated day.
- Kafka, the Flink cluster, Postgres (orders and serving) stay local in this feature (feature 10
  moves them to MSK, Managed Flink and RDS).

## Users and scenarios
- **Interviewer:** watches the same pipeline run against AWS, then sees it destroyed, at a cost
  of cents.
- **Developer:** switches back to the local lakehouse with the same configuration switch.

## Acceptance criteria
- [ ] AC1: Given the two lakehouse settings (local, AWS), then the jobs, table definitions, dbt models and maintenance tasks are the same files; only configuration differs (a check fails if code branches on the environment).
- [ ] AC2: Given the owner's go-ahead, when the lakehouse is created, then the storage, the catalog databases, a query workgroup with a scan limit, a least-privilege access role and budget alerts exist; when it is removed, nothing billable remains (the bucket is emptied and deleted).
- [ ] AC3: Given the AWS lakehouse, then every bronze, silver, gold and maintenance-log table exists in the catalog with the same columns, partitioning and properties as locally, from one table definition.
- [ ] AC4: Given the AWS setting and a simulated hour of data, then bronze and silver rows written by the local Flink jobs are queryable in Athena, and their counts equal what was sent and deduplicated.
- [ ] AC5: Given the AWS setting, when gold is built and its data tests run with the Athena adapter, then they pass, and the KPIs of the simulated day equal the KPIs the local stack computes from the same seed.
- [ ] AC6: Given the AWS lakehouse, then compaction, snapshot expiry and orphan removal run by AWS-side configuration or the same maintenance tasks through Athena (ADR 0012 mapping).
- [ ] AC7: Given one demo session (create, one hour of data, gold build, remove), then its AWS cost is measured and stays under $1; the figure and how it was measured are recorded.
- [ ] AC8: Given the repository and the machine, then no AWS credential is ever written to a file the project creates, and no command creates AWS resources without an explicit confirmation.

## Out of scope
- MSK, MSK Connect, Managed Flink, RDS, ECS and `make aws-up` / `aws-down` for the full stack
  (feature 10).
- Serving and the control tower on AWS; dashboards on AWS.
- CI against AWS (it costs money); the AWS demo runs by hand.

## Decisions that change
- New ADR 0015: how the lakehouse switches environments (catalog settings, table creation,
  adapter, credentials, region).
- The local table DDL moves from Trino SQL to the shared table definition (open question 2).

## Open questions
1. **Region?** *Recommended:* `ap-southeast-1` (Singapore), closest to Ho Chi Minh City; all
   lakehouse services are available there.
2. **One table definition for two catalogs?** Trino DDL does not run in Athena (different syntax for
   Iceberg partitioning). Options:
   - (a) *Recommended:* define the tables once in Python with the Iceberg library and create them
     through the catalog API, in both the REST catalog and Glue (partition transforms, sort order
     and properties included). The Trino init SQL for tables goes away.
   - (b) Keep the Trino DDL and add an Athena copy: two files per table to keep in step.
3. **Credentials?** *Recommended:* the owner's own AWS login in the shell (an SSO or profile
   session); the containers receive short-lived credentials from the environment at start and
   nothing is stored. The demo role created in AC2 is the only permission the jobs use.
4. **Maintenance on AWS?** *Recommended:* the Glue table optimizers for compaction, snapshot
   retention and orphan deletion (managed, no Trino needed); retention deletes run as the same
   task through Athena. Locally nothing changes.
5. **Which engine for the local services in AWS mode?** *Recommended:* Trino is not started in AWS
   mode; every query (dbt, status commands, the demo check) goes to Athena, so the demo proves
   Athena, not Trino over Glue.
6. **dbt models on Athena?** *Recommended:* the same models; the few engine details that differ
   (partition configuration, time zone and Bing tile functions) go through the existing macros,
   with one branch per adapter in the macros only.
7. **Cost guard?** *Recommended:* the Athena workgroup stops queries over 1 GB scanned, budget
   alerts at $5 and $20 for this feature (the account-wide $20/$50 of ADR 0005 stay), and the demo
   command prints the estimated cost before it creates anything.

# 0005: Local-first development, AWS managed services on demand
Date: 2026-11-17 · Updated: 2026-11-18 · Status: Final

## Context
Personal budget (about $190 of AWS credit), yet the project must prove it runs on AWS
managed services. The laptop is a Ryzen 5 5600H with 12 threads and 14 GB RAM, running WSL2.
The full Docker stack needs about 8 GB.

## Decision
Two environments, with **one codebase** and differences only in config:
| Env | Where | Runs | When |
|---|---|---|---|
| `local` | WSL2 (raised to 10 GB / 8 vCPU) | full Docker Compose stack, all data tiers (ADR 0009) | daily, $0 |
| `managed` | MSK Serverless, MSF, RDS, S3/Glue/Athena, ECS | the same jobs on managed services | once stable, for demos; destroyed after |

- Compose **profiles** keep memory in check: `core` (Kafka, Flink, MinIO, catalog, Trino),
  `cdc` (Postgres, Debezium) and `ui` (Grafana, Prometheus) start only when needed.
- The lakehouse switches from MinIO/REST/Trino to S3/Glue/Athena by config alone
  (roadmap item 9).
- The `managed` env is created by `make aws-up` and removed by `make aws-down`.
- AWS Budgets alerts at $20 and $50.

## Options considered
- **Local-first + managed on demand (chosen):** free iteration, and the managed deploy proves cloud skills.
- **EC2 devbox for the stack:** stronger hardware and repeatable benchmarks, but ~$25–35/month and a code-sync step. Kept as the fallback if the laptop cannot run the `standard` tier.
- **Local only:** free, but no proof of cloud skills.
- **Managed 24/7:** most realistic, but MSK + Flink cost tens to hundreds of $/month.

## Consequences
+ Day-to-day cost is $0. Estimated managed spend is ~$4 per 2-hour demo (MSK Serverless dominates). 3–5 demos plus S3/Athena come to < $25 of credit. Verify with the AWS Pricing Calculator.
− WSL must be given more resources (`.wslconfig`), which leaves ~4 GB for Windows.
− Laptop benchmark numbers depend on that machine; the README states the exact hardware.
− To avoid NAT Gateway cost, Fargate tasks run in public subnets with no inbound rules (acceptable for a demo; documented as not production-grade).

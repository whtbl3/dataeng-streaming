# Spec 001: Walking skeleton (GPS → Kafka → Flink → Iceberg bronze → SQL)
Status: Implemented (2026-11-18) · Intent: roadmap item 1 · Author: project owner + Claude

<!-- WHAT and WHY only. No file names, no code, no SQL. -->

## Problem
Nothing runs end to end yet. Before adding CDC, schemas, dedup or alerts, we need the
thinnest real path through every core layer. That proves the stack starts, the pieces
talk to each other, and data can be queried. Every later feature then extends a working
pipeline instead of integrating many parts at once.

## Outcome
One command starts the core local stack. A small fleet simulator sends GPS pings to a
Kafka topic, and a Flink streaming job writes them unchanged (plus an ingest timestamp)
to an Iceberg bronze table on object storage. An analyst can query that table with SQL
within about a minute of the event. Lint and unit tests run in CI without Docker; the
end-to-end test runs locally against the live stack.

## Users and scenarios
- **Developer (me):** clones the repo, starts the stack, runs the simulator, and sees rows appear in SQL.
- **Interviewer reading the repo:** follows the README quick start and gets the same result.
- **CI:** blocks a PR when lint or unit tests fail.

## Acceptance criteria
- [ ] AC1: Given a fresh clone with Docker and the dev tools installed, when the developer starts the core stack, then every service reports healthy within 5 minutes and the GPS topic and bronze table exist.
- [ ] AC2: Given a seed and a fleet size, when the simulator generates pings, then every ping has a vehicle id, an event time in UTC, a position inside the Ho Chi Minh City bounding box, and a plausible speed (0–80 km/h). Event times per vehicle increase in steps of the ping interval, and the same seed produces identical output (unit test, no Docker).
- [ ] AC3: Given the stack is running, when the simulator sends N pings and stops, then the bronze table contains exactly N rows. Nothing is lost and nothing is duplicated on the happy path.
- [ ] AC4: Given the stack is running, when a ping is produced, then it is queryable in bronze within 60 seconds.
- [ ] AC5: Given the bronze table, when it is queried with SQL, then each row keeps the original payload fields plus an ingest timestamp and the Kafka partition and offset (for lineage and replay), and the table is partitioned by event day.
- [ ] AC6: Given a pull request, when CI runs, then lint and unit tests pass with the same command as locally, without Docker.
- [ ] AC7: Given the developer stops the stack, when they start it again, then the bronze data from before is still there.
- [ ] AC8: Given the core stack is running with the simulator at `dev` size (20 vans), then total container memory stays under 6 GB.

## Out of scope
- Avro and Schema Registry (feature 3): the skeleton uses JSON messages.
- Orders, Postgres, Debezium CDC (feature 2).
- Dedup, late or out-of-order handling, silver and gold layers (features 4, 7).
- Restart-with-exactly-once guarantees under failure (proven in feature 4).
- Realistic road-following movement (stretch). Vans move by bounded random walk.
- Grafana and Prometheus (feature 8). AWS of any kind (features 9–10).
- Backfill and the 100M benchmark (feature 5).

## Decisions that change
None. This implements ADRs 0001, 0002, 0003 and 0005 at their smallest scope.

## Resolved questions (approved 2026-11-18)
1. Message format before feature 3: **JSON**; Avro arrives in feature 3.
2. Van movement: **bounded random walk** inside the HCMC box at realistic speeds; OSM roads are a stretch goal.
3. Freshness target: **≤ 60 s** in bronze (~30 s checkpoint interval). The < 10 s alert path is feature 6.
4. End-to-end test: **`make e2e` locally + the manual CI `e2e` job**; `make check` stays Docker-free.
5. Prerequisite: WSL raised to 10 GB / 8 vCPU (2026-11-18).

# Plan 001: Walking skeleton (GPS → Redpanda → Flink → Iceberg bronze → Trino)
From: spec.md (Approved 2026-11-18) · Approved by: project owner (2026-11-18)

## Context
Spec `specs/001-walking-skeleton/spec.md` (Approved 2026-11-18) asks for the thinnest real
end-to-end slice: a fleet simulator → Kafka topic `gps.pings.v1` → Flink streaming job →
Iceberg bronze table on S3-compatible storage → SQL in Trino, plus unit tests in `make check`
and an e2e suite (`make e2e`). The repo has only docs and tooling today, so everything is new
code on branch `feat/001-walking-skeleton`. On approval, this plan is saved as
`specs/001-walking-skeleton/plan.md` and the task list as `tasks.md`.

## Pinned versions (verified 2026-11-18, each ≥ ~2 weeks old where possible)
| Component | Image / artifact |
|---|---|
| Kafka | `redpandadata/redpanda:v26.1.20` (single node, `--smp 1 --memory 768M`) |
| S3 local | `dhi.io/minio:RELEASE.2025-09-07T16-13-09Z-debian13` (Docker Hardened Image, non-root, no shell; needs `docker login dhi.io`) |
| Bucket init | `amazon/aws-cli:2.39.4` (`aws s3 mb` against the local endpoint) |
| Iceberg catalog | `apache/iceberg-rest-fixture:1.10.1` (SQLite file on a volume → survives restarts) |
| Flink | `flink:1.20.5-scala_2.12-java17` + JARs: `flink-sql-connector-kafka 3.4.0-1.20`, `iceberg-flink-runtime-1.20 1.10.1`, `iceberg-aws-bundle 1.10.1`, `hadoop-client-api/runtime 3.4.1` |
| SQL | `trinodb/trino:484` (Iceberg connector, REST catalog, native S3) |
| Python | `confluent-kafka 2.16.1` (runtime), `trino 0.340.0` (e2e group) |

## Files that change
- `simulator/` (new package)
  - `fleet.py`: a pure, deterministic generator, `generate_pings(seed, vans, interval_s, steps, end_ts)`. It does a bounded random walk inside the HCMC bbox (lat 10.65–10.90, lon 106.55–106.85), speed 0–80 km/h, and reflects at the box edges.
  - `producer.py`: JSON serialisation; key = `vehicle_id`; `confluent-kafka` with `acks=all` and idempotence on.
  - `__main__.py`: CLI `python -m simulator --vans 20 --interval 5 --minutes 2 --seed 42 [--realtime] [--prefix VAN]`. It prints the count produced.
  - Fast mode (the default) emits events whose times end at now. `--realtime` sleeps between pings and is used by `make demo`.
- `infra/local/docker-compose.yml`: services `minio`, `minio-init`, `redpanda`, `redpanda-init` (rpk creates topic `gps.pings.v1`, 3 partitions, 7-day retention), `iceberg-rest`, `trino`, `trino-init` (bronze DDL), `jobmanager`, `taskmanager`, `flink-submit`.
  - Every value comes from `${VAR:-default}`, so no `.env` file is required locally.
  - Healthchecks are set on every service, and memory limits total ≤ 6 GB.
  - Named volumes hold S3 data, the catalog DB and Flink checkpoints.
- `infra/local/flink/Dockerfile`: the Flink image plus the pinned JARs (downloaded with SHA-1 checks) and `gettext-base` for `envsubst`.
- `infra/local/flink/submit.sh`: renders the job SQL from env and submits it with `sql-client.sh -f`. It **skips the submit when a RUNNING job named `bronze_gps` already exists** (checked through the REST API).
- `flink/jobs/bronze_gps.sql` (the job):
  - The Iceberg REST catalog is set up with S3FileIO.
  - The Kafka source uses the JSON format, `event_ts TIMESTAMP_LTZ(3)` with a WATERMARK (−30 s, per `.claude/rules/flink.md`), and metadata columns `timestamp` (→ `ingest_ts`), `partition` and `offset`. It reads from `group-offsets` and falls back to `earliest`.
  - Checkpointing runs every 30 s, exactly-once.
  - The job does `INSERT INTO lakehouse.bronze.gps_pings`.
- `infra/local/trino/catalog/lakehouse.properties` and `jvm.config` (`-Xmx1G`).
- `infra/local/trino/init/001_bronze.sql`: creates the schema `bronze` and the table `gps_pings` (vehicle_id, event_ts, lat, lon, speed_kmh, heading_deg, ingest_ts, kafka_partition, kafka_offset) with `partitioning = ARRAY['day(event_ts)']`.
  - The table is created in Trino because Flink DDL cannot express hidden partition transforms.
- `tests/unit/test_fleet.py`, `tests/unit/test_producer.py`.
- `tests/integration/conftest.py` (Trino client, Kafka producer, wait/poll helpers) and `tests/integration/test_skeleton.py`.
- `pyproject.toml`: add `confluent-kafka` as a dependency and an `e2e` group with `trino`; pytest markers `e2e` and `restart`.
- `Makefile`:
  - `up` becomes `docker compose up -d --build --wait`.
  - `demo` runs the realtime simulator.
  - Add `stack-mem`, which prints the memory sum from `docker stats`.
  - `e2e` runs `pytest tests/integration -m "e2e and not restart"`, and a new `e2e-restart` runs the restart test.
- `.github/workflows/ci.yml`: the `e2e` job logs in to `dhi.io` using the secrets `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN`, then runs `make e2e`.
- `.env.example`: add `S3_IMAGE` and the MinIO/Kafka/Trino ports; local default credentials are non-secret.
- Docs:
  - `README.md`: prerequisites (`docker login dhi.io`) and the quick start.
  - `docs/tech-stack.md`: S3 local becomes MinIO (DHI); swapping to SeaweedFS is a `S3_IMAGE` change.
  - `docs/roadmap.md` and `docs/project-spec.md`: status set to Implemented at the end.

## Order of work
1. Unit tests for `fleet.py` (AC2), failing first → implement the generator.
2. Producer serialisation test → implement `producer.py` + CLI. `make check` should be green.
3. Compose, built up layer by layer, with a smoke check (`docker compose ps` healthy) after each: MinIO + bucket → Redpanda + topic → REST catalog → Trino + bronze DDL → Flink image + cluster + submit.
4. Integration tests AC1, AC3, AC4, AC5, AC8 → `make e2e` green; then AC7 → `make e2e-restart`.
5. CI e2e job, `.env.example`, README and tech-stack; update the roadmap and project spec; open the PR.

## Risks and rollback
- **Flink ↔ Iceberg classpath.** The Iceberg Flink runtime still needs Hadoop `Configuration`. Mitigation: ship `hadoop-client-api/runtime`. If that fails, try the Iceberg-recommended `flink-shaded-hadoop` JAR.
- **REST fixture persistence.** It defaults to in-memory SQLite. Mitigation: a SQLite file on a named volume (AC7 tests it). Fallback: JDBC catalog on `postgres:16` (already pulled; Postgres arrives in feature 2 anyway).
- **DHI MinIO runs as non-root.** Volume permissions may block writes. Mitigation: a named volume pre-owned through a one-shot `chown` init step, or `user:` override.
- **Timestamp precision.** Flink `TIMESTAMP_LTZ(3)` → Iceberg `timestamptz` (µs). Verify the implicit cast; otherwise `CAST` in the INSERT.
- **Duplicate Flink jobs on re-run.** `submit.sh` is idempotent (it checks by job name).
- **Memory.** Limits are set per service. AC8 measures actual usage with `docker stats`.
- **DHI needs login.** CI uses repo secrets. The README documents the `S3_IMAGE` swap for people without a Docker account.
- **Rollback:** everything is new files on a feature branch. `make clean` removes containers and volumes; drop or revert the branch. No shared state, no AWS.

## Proof (acceptance criterion → test)
| AC | Proven by |
|---|---|
| AC1 healthy < 5 min, topic + table exist | CI e2e step `timeout 300 make up`; `test_stack_healthy_topic_and_table_exist` |
| AC2 valid, deterministic pings | `tests/unit/test_fleet.py` (bbox, speed, step = interval, same seed = same output, count = vans × steps) |
| AC3 N in → N rows, no loss or dups | `test_exactly_n_rows` (unique per-run vehicle prefix; count(*) and count(distinct partition, offset) = N) |
| AC4 visible ≤ 60 s | `test_freshness_under_60s` (marker ping, poll Trino) |
| AC5 payload + ingest_ts + partition/offset, day-partitioned | `test_bronze_schema_and_partitioning` (columns + `SHOW CREATE TABLE` contains `day(event_ts)`) |
| AC6 CI lint + unit, no Docker | CI `check` job green on the PR |
| AC7 data survives restart | `test_data_survives_restart` (marker `restart`: `down` without `-v` → `up` → earlier prefix still counted) |
| AC8 < 6 GB | `test_stack_memory_under_6gb` (sums `docker stats --no-stream` during a dev-size run) |

## Verification
- `make check`: lint + unit tests, no Docker (this is what CI runs).
- `make up` (timed) → `make e2e` → `make e2e-restart` → `make stack-mem`.
- Manual check: `make demo` for 2 minutes, then in Trino `SELECT count(*), max(event_ts) FROM lakehouse.bronze.gps_pings`. The Flink UI on :8081 shows `bronze_gps` RUNNING with checkpoints completing.

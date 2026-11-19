# Tasks 005: Scale
From: plan.md (approved 2026-11-19). Tests first in every task.

- [x] T1: spike S1–S4, outcome in plan.md
- [x] T2: Kafka 6 partitions, size bounds, partition discovery, producer zstd + Debezium compression (AC1, AC2, AC9)
- [x] T3: RocksDB + incremental checkpoints; `submit.sh` checkpoint resume + old-dir cleanup (AC4)
- [x] T4: unit tests, then `decode_gps_batch` + vectorized UDF (AC3)
- [x] T5: `table_admin.py` (unit tests first), silver GPS layout (stays upsert, see plan departures), GPS parallelism (AC11, AC12 props)
- [ ] T6: unit tests, then `maintain.py`; compose service; `make maintain`; `ops.maintenance_runs` (AC5–AC8)
- [ ] T7: `make table-health` (AC10); expiry vs reader in `submit.sh` (AC13); memory (AC15)
- [ ] T8: ADR 0012; ADR 0009/0010/0011, intent, requirements, project-spec, roadmap, README, flink.md, explainer
- [ ] T9: `make check`, `make e2e`, `make e2e-restart` green; spec Implemented; PR

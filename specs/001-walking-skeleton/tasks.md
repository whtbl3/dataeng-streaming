# Tasks 001: Walking skeleton

<!-- Small, independently testable. Test first. Tick as you go. -->
- [x] T1: failing unit tests for the fleet generator (AC2)
- [x] T2: implement `simulator/fleet.py` until T1 passes
- [x] T3: failing test for JSON serialisation and key → implement `producer.py` + CLI; `make check` green
- [ ] T4: compose MinIO (DHI) + bucket init; healthy
- [ ] T5: compose Redpanda + topic init; healthy; produce with the simulator
- [ ] T6: compose Iceberg REST (persistent) + Trino + bronze DDL; table visible in Trino
- [ ] T7: Flink image with JARs + JM/TM + idempotent submit; job RUNNING
- [ ] T8: integration tests AC1, AC3, AC4, AC5 → `make e2e` green
- [ ] T9: AC8 memory test + memory limits tuned
- [ ] T10: AC7 restart test → `make e2e-restart` green
- [ ] T11: CI e2e job (dhi.io login), `.env.example`, README, tech-stack
- [ ] T12: `make check` green; roadmap + project-spec updated; spec Status: Implemented; open PR

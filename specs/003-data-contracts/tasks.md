# Tasks 003: Data contracts

<!-- Small, independently testable. Test first. Tick as you go. -->
- [x] T1: tests, then `contracts/bounds`, `wire`, `validate` (simulator imports bounds)
- [x] T2: tests, then `contracts/compat` + schemas v1/v2 + the `make check` step
- [x] T3: tests, then `contracts/registry` + `decode_gps` (fake registry)
- [x] T4: tests, then producer Avro encoding + `--invalid-rate` injection; `make check` green
- [x] T5: Flink image with PyFlink + UDF smoke test + `EXPLAIN` sub-plan check
- [x] T6: registration in `redpanda-init`, DLQ topics + table, `bronze_gps` rewrite; 001 e2e green
- [x] T7: Connect image + Avro converter; capture the CDC fixture; tests, then `decode_cdc`; `bronze_cdc` rewrite; 002 e2e green
- [x] T8: e2e AC1, AC3, AC4, AC6, AC8 + `make dlq-status`; AC9 memory
- [x] T9: ADR 0010, ADR 0008 and spec AC1 update, project-spec, tech-stack, README, roadmap
- [x] T10: `make check` + `make e2e` + `make e2e-restart` green; spec Implemented; PR

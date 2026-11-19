# Tasks 004: Silver (dedup, event-time watermarks, chaos mode)

<!-- Small, independently testable. Test first. Tick as you go. -->
- [x] T1: tests, then `simulator/chaos.py` (AC1)
- [x] T2: CLI chaos flags + `--late-delivery-rate` rename; tests updated; `make check` green
- [x] T3: producer record timestamp = send time; unit test; e2e 001–003 green
- [x] T4: spike R1, with the outcome recorded in plan.md
- [x] T5: `004_silver.sql` + `silver_gps.sql` + compose submit; e2e AC2, AC3, AC4, AC9
- [x] T6: `silver_orders.sql`; e2e AC5, AC6
- [ ] T7: `make silver-status` + e2e AC10
- [ ] T8: restart AC7, `make silver-rebuild` + AC8, memory AC11
- [ ] T9: ADR 0011, project-spec, requirements, roadmap, README, explainer status
- [ ] T10: `make check`, `make e2e`, `make e2e-restart` green; spec Implemented; PR

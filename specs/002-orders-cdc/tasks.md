# Tasks 002: Orders with CDC

<!-- Small, independently testable. Test first. Tick as you go. -->
- [x] T1: tests for `world.py` + seed-SQL drift → implement `world.py` and `seed-sql`
- [x] T2: failing tests for `day.py` (AC2, AC3, AC4, re-targeted 001 GPS properties)
- [x] T3: implement `day.py` until T2 passes; remove the random walk
- [x] T4: `orders_db.py` + CLI subcommands with fake-cursor tests; `make check` green
- [ ] T5: compose Postgres (schema, seed, logical WAL); healthy; `make psql`
- [ ] T6: compose Connect + connector registration + CDC topics; snapshot visible in Kafka
- [ ] T7: bronze CDC DDL + Flink `bronze_cdc` job (submit handles 2 jobs, slots = 4); rows in Trino
- [ ] T8: e2e AC1, AC5, AC6, AC9
- [ ] T9: e2e AC7 (connector restart) + `make cdc-status` + AC8 test
- [ ] T10: CI e2e (clean → no volume), `.env.example`, README, project-spec, roadmap
- [ ] T11: `make check` + `make e2e` + `make e2e-restart` green; spec Implemented; PR

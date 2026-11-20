# Tasks 009: AWS lakehouse
From: plan.md (approved 2026-11-20). Tests first in every task.

- [x] T1: spikes S1-S4, outcome in plan.md
- [x] T2: catalog options from env (jobs, submit, compose); local regression
- [x] T3: tests, then `tables.py` replaces Trino DDL + `table_admin.py`; `lakehouse-init`; local e2e
- [x] T4: dbt aws target, adapter macros, image; `make dbt-ci`
- [x] T5: Terraform lakehouse + optimizer role + budgets; make targets with confirmation; `terraform plan`
- [x] T6: ~~`aws_demo.py`~~ narrowed by the owner to a short smoke test; **owner's OK** (2026-11-20) → up, tables, Athena checks, destroy
- [x] T7: ADR 0015, docs; `make check`, `make e2e`; spec Implemented; PR

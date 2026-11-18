---
paths: ["dbt/**"]
---
# dbt rules
- SQL must run on both Trino (local) and Athena (AWS): no engine-specific functions
  without a cross-database macro.
- Gold models are incremental with a `unique_key`, so late and duplicate data merge
  instead of appending (ADR 0006).
- Every model has at least `not_null` + `unique` tests on its key; sources have freshness checks.
- Gold rows carry `is_final`. Only the nightly close job sets it to true.
- `profiles.yml` is committed and must read every host and credential with `env_var()`. Never put literals in it.

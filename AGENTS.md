# Delivery Control Tower

Real-time last-mile delivery tracking on a streaming lakehouse: Kafka → Flink → Iceberg → dbt → Grafana, local-first, deployable to AWS.
Goal and scope: @docs/intent.md
Full spec: docs/project-spec.md · Stack: docs/tech-stack.md · Decisions: docs/decisions/

## Commands
- Check everything (same as CI): `make check`
- Lint: `make lint` · Test: `make test` · Format: `make format` · Install: `make install`
- Local stack: `make up` / `make down` · Demo data: `make demo` · End-to-end tests: `make e2e` (needs `make up`)

## Rules that are easy to break
- IMPORTANT: Never run `terraform apply`/`destroy`, `make aws-up` or `make aws-down` without the user's explicit OK in this session. They cost real money.
- Never read or edit `.env`, `*.tfvars`, `*.tfstate`, `profiles.yml` or other secret files; use the `.example` files.
- If a test fails, fix the code, not the test.
- Local vs AWS differences live only in config/env files, never in code (ADR 0005).
- Python 3.11 only (PyFlink limit). Event time is `event_ts` (UTC, `timestamp(3)`); never use processing time for business logic.
- Every architecture choice that is hard to reverse gets an ADR in `docs/decisions/`.

## Workflow
- New feature: run `/new-feature`. Do not write code before the spec is approved.
- One-sentence change (typo, small fix, docs): just do it, no spec.
- Non-trivial change: use plan mode first.
- Before saying "done": run `make check` and paste the output.
- Commits: small, imperative mood, in English.

## Language
- Code, comments, docs, commit messages: English. Chat with the user in Vietnamese.

## When to edit this file
- Codex repeats a mistake → add one line here.
- A rule must never be broken → make it a hook in `.Codex/hooks/` instead.

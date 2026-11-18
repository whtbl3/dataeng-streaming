# Rule: `make check` must run exactly what CI runs.
PROFILES ?= cdc
COMPOSE := docker compose -f infra/local/docker-compose.yml $(addprefix --profile ,$(PROFILES))
TF      := terraform -chdir=infra/aws

.PHONY: install format lint test check up down clean demo e2e e2e-restart stack-mem psql cdc-status aws-plan aws-up aws-down

## ---- Code quality (CI) -------------------------------------------------
install:
	uv sync --all-groups

format:
	uv run ruff format .
	uv run ruff check --fix .
	@if [ -d infra/aws ]; then $(TF) fmt -recursive; fi

lint:
	uv run ruff check .
	uv run ruff format --check .
	@if [ -d infra/aws ]; then $(TF) fmt -check -recursive; fi

test:
	bash scripts/test_hooks.sh
	uv run pytest -q tests/unit

check: lint test
	@echo "check: all green"

## ---- Local stack ($0) -------------------------------------------------
up:               # start services; `run` waits for every depends_on condition, then submits jobs
	$(COMPOSE) up -d --build --remove-orphans
	$(COMPOSE) run --rm flink-submit

down:
	$(COMPOSE) --profile jobs down

clean:            # also deletes local data volumes
	$(COMPOSE) --profile jobs down -v

demo:             # live delivery day at dev size (20 vans, 500 orders/day); Ctrl-C to stop
	uv run python -m simulator day --realtime --vans 20

e2e:              # needs `make up`
	uv run pytest -q tests/integration -m "e2e and not restart"

e2e-restart:      # restarts the stack (slow)
	uv run pytest -q tests/integration -m restart

cdc-status:       # connector + task state, and how far the replication slot lags behind
	@curl -fsS http://localhost:8083/connectors/ct-orders/status; echo
	@$(COMPOSE) exec -T postgres psql -U ct -d control_tower -c \
	  "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots"

psql:
	$(COMPOSE) exec postgres psql -U ct -d control_tower

stack-mem:
	@docker stats --no-stream --format '{{.Name}}\t{{.MemUsage}}' $$($(COMPOSE) ps -q)

## ---- AWS (costs money: destroy after each demo, ADR 0005) -------------
aws-plan:
	$(TF) init -input=false && $(TF) plan

aws-up:
	$(TF) init -input=false && $(TF) apply

aws-down:
	$(TF) destroy

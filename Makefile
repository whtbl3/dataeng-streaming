# Rule: `make check` must run exactly what CI runs.
COMPOSE := docker compose -f infra/local/docker-compose.yml
TF      := terraform -chdir=infra/aws

.PHONY: install format lint test check up down clean demo e2e aws-plan aws-up aws-down

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
up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

clean:            # also deletes local data volumes
	$(COMPOSE) down -v

demo:
	uv run python -m simulator

e2e:
	uv run pytest -q tests/integration

## ---- AWS (costs money: destroy after each demo, ADR 0005) -------------
aws-plan:
	$(TF) init -input=false && $(TF) plan

aws-up:
	$(TF) init -input=false && $(TF) apply

aws-down:
	$(TF) destroy

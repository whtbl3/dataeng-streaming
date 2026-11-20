# Rule: `make check` must run exactly what CI runs.

# Lakehouse (spec 009): local = MinIO + REST catalog + Trino (default); aws = S3 + Glue + Athena,
# with Kafka, Flink and Postgres still local. Only configuration differs (ADR 0015).
LAKEHOUSE ?= local
# AWS_PROFILE: the demo role the jobs use (infra/aws/lakehouse.env.example);
# AWS_ADMIN_PROFILE: your own login, to create and remove the AWS lakehouse
AWS_PROFILE ?= ct-demo
AWS_ADMIN_PROFILE ?= ct-admin
ifeq ($(LAKEHOUSE),aws)
PROFILES ?= cdc
export LAKEHOUSE_ENV := ../aws/lakehouse.env
# short-lived credentials from your AWS login, as environment variables only (never a file)
CREDS = eval "$$(aws configure export-credentials --profile $(AWS_PROFILE) --format env)" &&
else
PROFILES ?= lake cdc
endif
ADMIN_CREDS = eval "$$(aws configure export-credentials --profile $(AWS_ADMIN_PROFILE) --format env)" &&
COMPOSE := docker compose -f infra/local/docker-compose.yml $(addprefix --profile ,$(PROFILES))

# Terraform: the installed binary, or the pinned image when none is installed
TF_IMAGE := hashicorp/terraform:1.9.8
TF_BIN   := $(if $(shell command -v terraform),terraform,docker run --rm -i -v $(CURDIR):/repo -w /repo -u $(shell id -u):$(shell id -g) -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN $(TF_IMAGE))
TF       := $(TF_BIN) -chdir=infra/aws
TF_LAKE  := $(TF_BIN) -chdir=infra/aws/lakehouse

.PHONY: install format lint test check up down clean demo e2e e2e-restart stack-mem psql cdc-status dlq-status silver-status silver-rebuild gold close reclose gold-status gold-test dbt-ci tower-status maintain table-health aws-plan aws-up aws-down aws-lake-plan aws-lake-up aws-lake-down

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
	uv run python -m contracts.compat schemas/
	uv run pytest -q tests/unit

check: lint test
	@echo "check: all green"

## ---- Local stack ($0) -------------------------------------------------
up:               # start services; `run` waits for every depends_on condition, then submits jobs (LAKEHOUSE=aws: spec 009)
	$(CREDS) $(COMPOSE) up -d --build --remove-orphans
	$(CREDS) $(COMPOSE) run --rm flink-submit

down:             # stops the Flink jobs with a savepoint first, so `make up` resumes exactly
	-$(CREDS) $(COMPOSE) run --rm --no-deps flink-submit /opt/flink/bin/stop_jobs.sh
	$(COMPOSE) --profile lake --profile jobs down

clean:            # also deletes local data volumes
	$(COMPOSE) --profile jobs down -v

demo:             # live delivery day at dev size (20 vans, 500 orders/day, 2 abnormal stops); Ctrl-C to stop
	uv run python -m simulator day --realtime --vans 20 --abnormal-stops 2

e2e:              # needs `make up`
	uv run pytest -q tests/integration -m "e2e and not restart"

e2e-restart:      # restarts the stack (slow)
	uv run pytest -q tests/integration -m restart

cdc-status:       # connector + task state, and how far the replication slot lags behind
	@curl -fsS http://localhost:8083/connectors/ct-orders/status; echo
	@$(COMPOSE) exec -T postgres psql -U ct -d control_tower -c \
	  "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots"

dlq-status:       # rejected events per source topic and reason (bronze.dlq_events, spec 003)
	@$(COMPOSE) exec -T trino trino --output-format ALIGNED --execute \
	  "SELECT source_topic, reason, count(*) AS events, max(ingest_ts) AS last_seen FROM lakehouse.bronze.dlq_events GROUP BY 1, 2 ORDER BY 1, 2"

silver-status:    # per silver table and local day: rows kept, copies removed, late rows (spec 004)
	@$(COMPOSE) exec -T trino trino --output-format ALIGNED < infra/local/trino/silver_status.sql

silver-rebuild:   # drop silver and rebuild it from bronze with the same jobs (spec 004 AC8)
	COMPOSE="$(COMPOSE)" infra/local/silver_rebuild.sh

gold:             # build gold now: provisional rows for open days, last 2 h of commits (spec 007)
	$(COMPOSE) run --rm --no-deps dbt --once

close:            # close a local day: make close DAY=2026-11-18 (final rows, closed_days; idempotent)
	$(COMPOSE) run --rm --no-deps dbt --close $(DAY)

reclose:          # restate a closed day, logged in closed_days.restatements: make reclose DAY=…
	$(COMPOSE) run --rm --no-deps dbt --reclose $(DAY)

gold-status:      # per local day: closed orders, on-time rate, final rows, close and late counts
	@$(COMPOSE) exec -T trino trino --output-format ALIGNED < infra/local/trino/gold_status.sql

dbt-ci:           # dbt unit tests + SQL lint on a throwaway Trino, no stack needed (CI, spec 007)
	scripts/dbt_ci.sh

gold-test:        # dbt data tests on the real gold tables (fails on a broken model, spec 007 AC11)
	$(COMPOSE) run --rm --no-deps dbt --test

tower-status:     # control tower: open orders with an ETA, open alerts by type, newest position per van (spec 006)
	@$(COMPOSE) exec -T postgres psql -X -q -A -F ' | ' -P footer=off -U ct -d control_tower \
	  < infra/local/postgres/tower_status.sql

maintain:         # run table maintenance now: make maintain [TASK=compact|manifests|expire|orphans|retention] [TABLE=ns.t]
	$(COMPOSE) run --rm --no-deps -e SNAPSHOT_RETENTION -e ORPHAN_RETENTION maintenance --once \
	  $(if $(TASK),--task $(TASK)) $(if $(TABLE),--table $(TABLE))

table-health:     # per table: data files, delete files, average file size, snapshots, last maintenance (spec 005)
	@$(COMPOSE) exec -T trino trino --output-format ALIGNED < infra/local/trino/table_health.sql

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

## ---- AWS lakehouse (spec 009): S3 + Glue + Athena, created only on demand -------
aws-lake-plan:    # what aws-lake-up would create (free; log in first: aws login --profile ct-admin)
	$(ADMIN_CREDS) $(TF_LAKE) init -input=false && $(TF_LAKE) plan

aws-lake-up:      # create the AWS lakehouse; asks first. Remove it with aws-lake-down after the demo
	@echo "Creates in $(AWS_ADMIN_PROFILE)'s account (ap-southeast-1): an S3 bucket, 5 Glue databases,"
	@echo "an Athena workgroup (queries stop at 1 GB scanned), 2 IAM roles and 2 budgets (5 and 20 USD)."
	@echo "Nothing runs per hour: S3 ~0.025 USD/GB-month, Athena 5 USD/TB scanned, Glue optimizers per run."
	@read -r -p "Type yes to create them: " ok && [ "$$ok" = yes ]
	$(ADMIN_CREDS) $(TF_LAKE) init -input=false </dev/null && $(TF_LAKE) apply

aws-lake-down:    # remove everything aws-lake-up created, the bucket's data included
	$(ADMIN_CREDS) $(TF_LAKE) destroy

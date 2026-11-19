# dbt for the gold layer (spec 007, ADR 0014): the scheduler loop and `make gold|close|reclose`.
# Versions pinned, same as the `dbt` dependency group in pyproject.toml.
FROM python:3.11.11-slim
RUN pip install --no-cache-dir dbt-core==1.11.16 dbt-trino==1.11.0
# dbt writes target/ and logs/ here, so the project can be mounted read-only
ENV DBT_TARGET_PATH=/tmp/dbt-target DBT_LOG_PATH=/tmp/dbt-logs DBT_PROFILES_DIR=/tmp/dbt-profiles
WORKDIR /opt/dbt
ENTRYPOINT ["python", "-u", "/opt/dbt/scheduler.py"]

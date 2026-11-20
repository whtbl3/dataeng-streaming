# lakehouse-init (spec 009): creates the bronze, silver and ops Iceberg tables from
# infra/lakehouse/tables.py in the configured catalog (REST locally, Glue on AWS).
FROM python:3.11.11-slim
RUN pip install --no-cache-dir "pyiceberg[glue]==0.12.0"
WORKDIR /opt/lakehouse
COPY tables.py .
ENTRYPOINT ["python", "-u", "tables.py"]
CMD ["create"]

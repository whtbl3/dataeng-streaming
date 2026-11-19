# Flink 1.20 + the connector JARs the jobs need (ADR 0002). Versions are pinned and
# verified by SHA-1 from Maven Central. PyFlink (Python 3.11) runs the contract UDFs (ADR 0010).
FROM ghcr.io/astral-sh/uv:0.13.5 AS uv
FROM flink:1.20.5-scala_2.12-java17

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends gettext-base curl \
 && rm -rf /var/lib/apt/lists/*

ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eu; cd /opt/flink/lib; \
    fetch() { \
      url="$MAVEN/$1"; file="$(basename "$1")"; \
      curl -fsSL -o "$file" "$url"; \
      echo "$2  $file" | sha1sum -c -; \
    }; \
    fetch org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar 266330f8f26fec4957339b5ff31aa67b81c31462; \
    fetch org/apache/iceberg/iceberg-flink-runtime-1.20/1.10.1/iceberg-flink-runtime-1.20-1.10.1.jar 7c82d2c4111afc4f4891232d9b62279c232ae701; \
    fetch org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar 9c02c851a2356f287f040ed784170c758369d134; \
    fetch org/apache/hadoop/hadoop-client-api/3.4.1/hadoop-client-api-3.4.1.jar 7e4cfae7f5c85cffdbc21fdf749262fc78d4463a; \
    fetch org/apache/hadoop/hadoop-client-runtime/3.4.1/hadoop-client-runtime-3.4.1.jar 30ec07ceccb224b2ef17af34fbb593bff3e9e071; \
    chown flink:flink /opt/flink/lib/*.jar

# PyFlink for the Python UDFs in contracts/ (spec 003). Same version as the cluster.
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/python UV_NO_CACHE=1
RUN uv venv /opt/venv --python 3.11 \
 && uv pip install --python /opt/venv/bin/python apache-flink==1.20.5 fastavro==1.13.1 \
 && mv /opt/flink/opt/flink-python-1.20.5.jar /opt/flink/lib/ \
 && chown -R flink:flink /opt/venv /opt/python /opt/flink/lib
ENV PATH=/opt/venv/bin:$PATH

COPY --chown=flink:flink submit.sh stop_jobs.sh /opt/flink/bin/
RUN chmod +x /opt/flink/bin/submit.sh /opt/flink/bin/stop_jobs.sh && mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints
USER flink

# Flink 1.20 + the connector JARs the jobs need (ADR 0002). Versions are pinned and
# verified by SHA-1 from Maven Central.
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

COPY --chown=flink:flink submit.sh /opt/flink/bin/submit.sh
RUN chmod +x /opt/flink/bin/submit.sh && mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints
USER flink

# Debezium Kafka Connect + the Confluent Avro converter (spec 003), so change events are
# written as registry-framed Avro to the Redpanda Schema Registry. The zip is pinned by SHA-256.
FROM alpine:3.20 AS converter
ARG VERSION=8.4.0
ARG SHA256=c32a65d6175725ffd826ebfb1fe6a1a82f76401a33f0af343bf1a11be9e562de
RUN apk add --no-cache curl unzip \
 && curl -fsSL -o /tmp/c.zip "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/${VERSION}/confluentinc-kafka-connect-avro-converter-${VERSION}.zip" \
 && echo "${SHA256}  /tmp/c.zip" | sha256sum -c - \
 && unzip -q /tmp/c.zip -d /tmp \
 && mv "/tmp/confluentinc-kafka-connect-avro-converter-${VERSION}/lib" /avro-converter

FROM quay.io/debezium/connect:3.6.3.Final
COPY --from=converter /avro-converter /kafka/connect/confluent-avro-converter

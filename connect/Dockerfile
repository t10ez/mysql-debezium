FROM quay.io/debezium/connect:3.3

# Official ClickHouse sink connector (https://github.com/ClickHouse/clickhouse-kafka-connect)
ARG CLICKHOUSE_CONNECT_VERSION=v1.7.0
RUN curl -fsSL -o /tmp/clickhouse-kafka-connect.zip \
      "https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/${CLICKHOUSE_CONNECT_VERSION}/clickhouse-kafka-connect-${CLICKHOUSE_CONNECT_VERSION}.zip" \
 && unzip -q /tmp/clickhouse-kafka-connect.zip -d /kafka/connect/ \
 && rm /tmp/clickhouse-kafka-connect.zip

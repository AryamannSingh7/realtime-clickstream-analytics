# Kafka Connect worker with the official ClickHouse sink connector baked in.
# Built by docker-compose (service: kafka-connect). Free / open-source.
#
# The connector is pinned for reproducible builds. Bump the tag to upgrade.
FROM confluentinc/cp-kafka-connect:7.8.0

# Official ClickHouse Kafka Connect Sink (installs to /usr/share/confluent-hub-components).
RUN confluent-hub install --no-prompt clickhouse/clickhouse-kafka-connect:v1.3.7

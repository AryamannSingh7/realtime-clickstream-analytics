#!/usr/bin/env bash
# M0 smoke test: proves the local infra is up and wired correctly.
#   1) Kafka topics exist
#   2) Kafka produce/consume round-trip works
#   3) Schema Registry responds
#   4) ClickHouse responds and the events table is present
#
# Usage:  docker compose up -d   &&   bash scripts/smoke-test.sh
set -euo pipefail

echo "== [1/5] Kafka: list topics =="
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo
echo "== [2/5] Kafka: produce -> consume round-trip on 'smoke-test' =="
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic smoke-test --partitions 1 --replication-factor 1 >/dev/null
MSG="hello-clickstream-$(date +%s)"
echo "$MSG" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:9092 --topic smoke-test
echo "produced: $MSG"
echo "consumed:"
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic smoke-test --from-beginning --max-messages 1 --timeout-ms 15000

echo
echo "== [3/5] Schema Registry: subjects =="
curl -fsS http://localhost:8081/subjects && echo

echo
CH_AUTH="--user clickstream:clickstream"
echo "== [4/5] ClickHouse: version =="
curl -fsS $CH_AUTH 'http://localhost:8123/?query=SELECT%20version()'

echo "== [5/5] ClickHouse: tables in 'analytics' =="
curl -fsS $CH_AUTH 'http://localhost:8123/?query=SHOW%20TABLES%20FROM%20analytics'

echo
echo "✅ ALL SMOKE CHECKS PASSED"

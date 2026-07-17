#!/usr/bin/env bash
# M7.4 — horizontal scaling of the stream-processor.
#
# Measures the Kafka Streams consumer group's aggregate source-consumption rate and
# backlog for the CURRENT number of running stream-processor replicas, so 1 / 2 / 3
# instances can be compared. Consumption rate is the delta of the group's committed
# offsets on clickstream.events.raw over the sample window (instance-count agnostic).
#
# Scale between runs with the override, then re-run this:
#   docker compose -f docker-compose.yml -f docker-compose.scale.yml \
#       up -d --no-deps --scale stream-processor=N stream-processor
#   ./capture-scaling.sh [targetEps] [sampleSec]      # defaults: 30000 30
#
# targetEps is driven deliberately OVER the single-node ceiling so the measured
# consumption rate reflects the pipeline's max throughput, not just the arrival rate.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require docker curl

TARGET="${1:-30000}"
SAMPLE="${2:-30}"
WARMUP="${SCALE_WARMUP:-20}"
GROUP="clickstream-stream-processor"
TOPIC="clickstream.events.raw"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-kafka:29092}"
ts="$(now_iso | tr -d ':-')"
out="$RESULTS_DIR/scaling_${ts}.tsv"
mkdir -p "$RESULTS_DIR"

describe() {
  docker exec "$KAFKA_CONTAINER" kafka-consumer-groups \
    --bootstrap-server "$BOOTSTRAP" --describe --group "$GROUP" 2>/dev/null
}
# Sum committed CURRENT-OFFSET (col 4) over the source topic's partitions.
offsets() { describe | awk -v t="$TOPIC" 'NR>1 && $2==t && $4 ~ /^[0-9]+$/ {s+=$4} END{printf "%d", s+0}'; }

instances="$(docker ps --filter 'name=stream-processor' --format '{{.Names}}' | grep -c . || true)"

gen_rate "$TARGET"
echo "# instances=${instances} target=${TARGET}eps warmup=${WARMUP}s sample=${SAMPLE}s"
sleep "$WARMUP"

o0="$(offsets)"; sleep "$SAMPLE"; o1="$(offsets)"
consume_eps=$(( (o1 - o0) / SAMPLE ))

snap="$(describe)"
lag="$(printf '%s\n' "$snap" | awk -v t="$TOPIC" 'NR>1 && $2==t && $6 ~ /^[0-9]+$/ {l+=$6} END{printf "%d", l+0}')"
consumers="$(printf '%s\n' "$snap" | awk -v t="$TOPIC" 'NR>1 && $2==t && $7!="" && $7!="-" {print $7}' | sort -u | grep -c . || true)"

[ -f "$out" ] || printf 'instances\ttarget_eps\tconsume_eps\tsource_lag\tconsumers\n' | tee "$out"
printf '%s\t%s\t%s\t%s\t%s\n' "$instances" "$TARGET" "$consume_eps" "$lag" "$consumers" | tee -a "$out"

#!/usr/bin/env bash
# Consumer-group lag snapshot, read straight off the broker. Sums LAG across all
# partitions for each group — the Streams app's backlog (and the ClickHouse sink's)
# under load. A bounded / near-zero lag means the pipeline is keeping up.
#
#   ./capture-lag.sh [group ...]   # default: the Streams app + ClickHouse sink groups
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require docker

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-kafka:29092}"

groups=("$@")
if [ "${#groups[@]}" -eq 0 ]; then
  groups=("clickstream-stream-processor" "connect-clickhouse-sink")
fi

printf '%-34s %-12s %-12s\n' "group" "total_lag" "partitions"
for g in "${groups[@]}"; do
  # Columns: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID ...
  # Sum column 6 over data rows whose LAG is numeric (skips header and idle "-" rows).
  summary="$(docker exec "$KAFKA_CONTAINER" kafka-consumer-groups \
      --bootstrap-server "$BOOTSTRAP" --describe --group "$g" 2>/dev/null \
    | awk 'NR>1 && $6 ~ /^[0-9]+$/ { lag += $6; parts++ } END { printf "%d %d", lag+0, parts+0 }')"
  printf '%-34s %-12s %-12s\n' "$g" "${summary% *}" "${summary#* }"
done

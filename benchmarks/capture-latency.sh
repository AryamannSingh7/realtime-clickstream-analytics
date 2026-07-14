#!/usr/bin/env bash
# End-to-end produced -> visible latency for the non-windowed ingest path, computed
# in ClickHouse as (ingested_at - event_time) over rows ingested in the last WINDOW
# seconds. Filtered to < CAP_MS to isolate the on-time path from the generator's
# injected ~5% late events (backdated up to 60s), which would otherwise inflate the
# upper tail. The windowed/suppressed rollups are a separate story (see note below).
#
#   ./capture-latency.sh [windowSeconds]      # default 60
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require curl

WINDOW="${1:-60}"
CAP_MS="${LATENCY_CAP_MS:-2000}"

row="$(ch_query "
  SELECT count(),
         round(quantile(0.50)(lat)), round(quantile(0.95)(lat)), round(quantile(0.99)(lat)),
         max(lat), round(avg(lat), 1)
  FROM (
    SELECT dateDiff('millisecond', event_time, ingested_at) AS lat
    FROM analytics.events
    WHERE ingested_at >= now64(3) - INTERVAL ${WINDOW} SECOND
      AND lat BETWEEN 0 AND ${CAP_MS}
  ) FORMAT TSV")"

IFS=$'\t' read -r n p50 p95 p99 mx avg <<< "$row"

echo "# produced -> visible (ingest path), last ${WINDOW}s, on-time (< ${CAP_MS}ms):"
printf 'samples=%s  p50=%sms  p95=%sms  p99=%sms  max=%sms  avg=%sms\n' \
  "${n:-0}" "${p50:-?}" "${p95:-?}" "${p99:-?}" "${mx:-?}" "${avg:-?}"
echo "# note: windowed rollups (metrics.1m, top-N) become visible ~one window length"
echo "#       (60s + grace) after event time by design — inherent, not a latency bug."

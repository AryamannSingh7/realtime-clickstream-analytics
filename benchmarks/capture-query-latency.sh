#!/usr/bin/env bash
# M7.3 — OLAP query latency over a bounded synthetic dataset.
#
# Loads ~ROWS rows into a dedicated analytics.events_bench table (recent event_time so
# the 30-day TTL keeps them; ~1M distinct anonymous_ids for a real uniqCombined test),
# then measures ClickHouse *engine* time for the queries the analytics-api serves —
# funnel (windowFunnel), unique visitors (uniqCombined), event timeseries, top pages —
# plus a retention() query. Timing is read from system.query_log (query_duration_ms),
# not wall-clock, so it excludes HTTP/JDBC overhead and reports the pure query cost.
#
# Each query is run once cold (mark + uncompressed caches dropped) and RUNS times warm;
# the warm median/min/max and rows-scanned are reported. Memory is bounded per query.
# The generator is paused for the duration so nothing competes, and resumed on exit.
#
#   ./capture-query-latency.sh [rows] [runs]      # defaults: 10000000 5
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require curl

ROWS="${1:-10000000}"
RUNS="${2:-5}"
MAX_MEM="${CH_MAX_MEMORY:-4000000000}"   # per-query cap (~4 GB); dataset is ~450 MiB
TABLE="analytics.events_bench"
ts="$(now_iso | tr -d ':-')"
out="$RESULTS_DIR/query_latency_${ROWS}rows_${ts}.tsv"
mkdir -p "$RESULTS_DIR"

# Resume the generator whatever happens (it was paused for a clean measurement).
cleanup() { gen_resume 2>/dev/null || true; }
trap cleanup EXIT

# Run a query under a tagged id + memory cap, then read its engine time and rows read
# back from query_log. Prints "duration_ms<TAB>read_rows".
ch_measure() {
  local sql="$1" qid="qlat_${RANDOM}${RANDOM}"
  curl -fsS "$CLICKHOUSE_URL/?user=${CLICKHOUSE_USER:-clickstream}&password=${CLICKHOUSE_PASSWORD:-clickstream}&database=${CLICKHOUSE_DB:-analytics}&query_id=${qid}&max_memory_usage=${MAX_MEM}" \
    --data-binary "$sql" >/dev/null
  ch_query "SYSTEM FLUSH LOGS" >/dev/null
  ch_query "SELECT query_duration_ms, read_rows FROM system.query_log
            WHERE query_id='${qid}' AND type='QueryFinish'
            ORDER BY event_time DESC LIMIT 1 FORMAT TSV"
}

# bench <label> <sql>: one cold run + RUNS warm runs -> label, cold, warm median/min/max, rows
bench() {
  local label="$1" sql="$2"
  ch_query "SYSTEM DROP MARK CACHE" >/dev/null
  ch_query "SYSTEM DROP UNCOMPRESSED CACHE" >/dev/null
  local cold_ms rows; IFS=$'\t' read -r cold_ms rows <<< "$(ch_measure "$sql")"

  local durs=() i ms rr
  for ((i = 0; i < RUNS; i++)); do
    IFS=$'\t' read -r ms rr <<< "$(ch_measure "$sql")"
    durs+=("$ms")
  done
  local stats
  stats="$(printf '%s\n' "${durs[@]}" | sort -n | awk '
    {a[NR]=$1}
    END{ med = (NR%2) ? a[(NR+1)/2] : int((a[int(NR/2)]+a[int(NR/2)+1])/2);
         printf "%d\t%d\t%d", med, a[1], a[NR] }')"
  printf '%s\t%s\t%s\t%s\n' "$label" "$cold_ms" "$stats" "$rows" | tee -a "$out"
}

echo "# Loading ${TABLE} to >= ${ROWS} rows if needed..."
existing="$(ch_query "SELECT count() FROM ${TABLE}" 2>/dev/null || echo 0)"
if [ "${existing:-0}" -lt "$ROWS" ]; then
  ch_query "DROP TABLE IF EXISTS ${TABLE}" >/dev/null
  ch_query "CREATE TABLE ${TABLE} AS analytics.events" >/dev/null
  ch_query "
    INSERT INTO ${TABLE}
      (event_id, event_type, anonymous_id, event_time, path,
       device_type, device_os, device_browser, geo_country, geo_city)
    SELECT
      toString(number),
      ['page_view','page_view','page_view','page_view','add_to_cart','add_to_cart','checkout_start','purchase','search','login'][(rand(1) % 10) + 1],
      concat('anon-', toString(rand(2) % 1000000)),
      now() - toIntervalSecond(rand(3) % 86400),
      ['/','/products','/product/42','/cart','/checkout','/search','/category/books'][(rand(4) % 7) + 1],
      ['desktop','mobile','tablet'][(rand(5) % 3) + 1],
      ['macos','windows','ios','android','linux'][(rand(6) % 5) + 1],
      ['chrome','safari','firefox','edge'][(rand(7) % 4) + 1],
      ['US','GB','DE','IN','BR','JP'][(rand(8) % 6) + 1],
      ['NYC','London','Berlin','Mumbai','SaoPaulo','Tokyo'][(rand(9) % 6) + 1]
    FROM numbers_mt(${ROWS})
    SETTINGS max_insert_threads = 4, max_memory_usage = ${MAX_MEM}" >/dev/null
fi

loaded="$(ch_query "SELECT count() FROM ${TABLE}")"
uniq_visitors="$(ch_query "SELECT uniqExact(anonymous_id) FROM ${TABLE}")"
size="$(ch_query "SELECT formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE database='analytics' AND table='events_bench' AND active")"
gen_pause 2>/dev/null || true

echo "# dataset: ${loaded} rows, ~${uniq_visitors} distinct visitors, ${size} on disk"
echo "# query engine latency (ms), cold = caches dropped, warm = median of ${RUNS} runs"
printf 'query\tcold_ms\twarm_med_ms\twarm_min_ms\twarm_max_ms\trows_scanned\n' | tee "$out"

bench "funnel_windowFunnel" "
  SELECT level, count() AS visitors FROM (
    SELECT anonymous_id, windowFunnel(1800)(toDateTime(event_time),
      event_type = 'page_view', event_type = 'add_to_cart',
      event_type = 'checkout_start', event_type = 'purchase') AS level
    FROM ${TABLE}
    WHERE event_time >= now() - INTERVAL 24 HOUR
    GROUP BY anonymous_id
  ) WHERE level > 0 GROUP BY level ORDER BY level FORMAT Null"

bench "unique_visitors_uniqCombined" "
  SELECT toStartOfHour(event_time) AS b, uniqCombined(anonymous_id) AS v
  FROM ${TABLE} WHERE event_time >= now() - INTERVAL 24 HOUR
  GROUP BY b ORDER BY b FORMAT Null"

bench "timeseries_by_type" "
  SELECT toStartOfHour(event_time) AS b, event_type, count() AS c
  FROM ${TABLE} WHERE event_time >= now() - INTERVAL 24 HOUR
  GROUP BY b, event_type ORDER BY b, event_type FORMAT Null"

bench "top_pages" "
  SELECT path, count() AS v FROM ${TABLE}
  WHERE event_type = 'page_view' AND event_time >= now() - INTERVAL 24 HOUR
  GROUP BY path ORDER BY v DESC, path LIMIT 10 FORMAT Null"

bench "retention_2buckets" "
  SELECT sumForEach(r) AS retention FROM (
    SELECT retention(
      event_time <  now() - INTERVAL 12 HOUR,
      event_time >= now() - INTERVAL 12 HOUR
    ) AS r
    FROM ${TABLE} WHERE event_time >= now() - INTERVAL 24 HOUR
    GROUP BY anonymous_id
  ) FORMAT Null"

echo "# wrote $out"
echo "# note: ${TABLE} left in place for re-runs; drop with: DROP TABLE ${TABLE}"

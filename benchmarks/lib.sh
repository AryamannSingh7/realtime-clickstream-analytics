#!/usr/bin/env bash
# Shared helpers for the benchmark harness. Source this from a bench script:
#
#   source "$(dirname "$0")/lib.sh"
#   require curl jq
#
# Endpoints default to a local `docker compose up` stack; override via env vars.
# Requires: curl only. Responses are parsed with sed/awk (ClickHouse HTTP returns
# raw values; service metrics are read from the plain-text Prometheus endpoint), so
# no jq is needed.

GENERATOR_URL="${GENERATOR_URL:-http://localhost:8089}"
ANALYTICS_API_URL="${ANALYTICS_API_URL:-http://localhost:8091}"
STREAM_URL="${STREAM_URL:-http://localhost:8090}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${RESULTS_DIR:-$BENCH_DIR/results}"

# Fail early with a clear message if a required CLI tool is missing.
require() {
  local cmd
  for cmd in "$@"; do
    command -v "$cmd" >/dev/null 2>&1 \
      || { echo "error: '$cmd' is required but not installed / on PATH" >&2; exit 1; }
  done
}

# --- generator control (mirrors GeneratorController) ---
gen_status()   { curl -fsS "$GENERATOR_URL/api/generator/status"; }
gen_rate()     { curl -fsS -X POST "$GENERATOR_URL/api/generator/rate?eps=$1" >/dev/null; }
gen_ramp()     { curl -fsS -X POST "$GENERATOR_URL/api/generator/ramp?from=$1&to=$2&durationSec=$3" >/dev/null; }
gen_burst()    { curl -fsS -X POST "$GENERATOR_URL/api/generator/burst?eps=$1&durationSec=$2" >/dev/null; }
gen_pause()    { curl -fsS -X POST "$GENERATOR_URL/api/generator/pause" >/dev/null; }
gen_resume()   { curl -fsS -X POST "$GENERATOR_URL/api/generator/resume" >/dev/null; }
gen_produced() { gen_status | sed -n 's/.*"totalProduced":\([0-9]*\).*/\1/p'; }
gen_target()   { gen_status | sed -n 's/.*"targetEps":\([0-9]*\).*/\1/p'; }

# Sum a Prometheus counter/gauge across all its label combinations:
#   prom_metric <base-url> <prometheus_metric_name>
# e.g.  prom_metric "$GENERATOR_URL" generator_events_produced_total
#       prom_metric "$STREAM_URL"    stream_events_consumed_total
# (Micrometer maps `generator.events.produced` -> `generator_events_produced_total`.)
prom_metric() {
  curl -fsS "$1/actuator/prometheus" \
    | awk -v m="^$2([ {])" '$0 ~ m {s += $NF} END { printf "%.0f\n", s + 0 }'
}

# Run a ClickHouse SQL query over HTTP and print the raw result.
ch_query() {
  curl -fsS "$CLICKHOUSE_URL/?user=${CLICKHOUSE_USER:-clickstream}&password=${CLICKHOUSE_PASSWORD:-clickstream}&database=${CLICKHOUSE_DB:-analytics}" \
    --data-binary "$1"
}

now_epoch() { date +%s; }
now_iso()   { date -u +%Y-%m-%dT%H:%M:%SZ; }

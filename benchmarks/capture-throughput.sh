#!/usr/bin/env bash
# Sustained end-to-end throughput. Drives a steady target rate and samples the
# effective rate at three pipeline stages so the bottleneck is visible:
#   produced (generator) -> consumed (stream-processor) -> ingested (ClickHouse).
# Writes a per-interval TSV to results/ and prints the sustained means + final lag.
#
#   ./capture-throughput.sh <eps> <durationSec> [sampleSec]
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require curl

EPS="${1:?usage: capture-throughput.sh <eps> <durationSec> [sampleSec]}"
DURATION="${2:?usage: capture-throughput.sh <eps> <durationSec> [sampleSec]}"
SAMPLE="${3:-5}"

ch_count() { ch_query "SELECT count() FROM analytics.events"; }
send_errors() { gen_status | sed -n 's/.*"sendErrors":\([0-9]*\).*/\1/p'; }

mkdir -p "$RESULTS_DIR"
out="$RESULTS_DIR/throughput_${EPS}eps_$(date -u +%Y%m%dT%H%M%SZ).tsv"
echo "# target ${EPS} eps for ${DURATION}s, sampling every ${SAMPLE}s -> $out"

gen_rate "$EPS"
sleep "$SAMPLE"   # let the new rate settle before taking the first baseline

p_prod="$(gen_produced)"
p_cons="$(prom_metric "$STREAM_URL" stream_events_consumed_total)"
p_ch="$(ch_count)"
t0="$(now_epoch)"; elapsed=0
sum_prod=0; sum_cons=0; sum_ch=0; samples=0

printf 'elapsed\tproduced_eps\tconsumed_eps\tingest_eps\tsend_errors\n' | tee "$out"
while [ "$elapsed" -lt "$DURATION" ]; do
  sleep "$SAMPLE"
  c_prod="$(gen_produced)"
  c_cons="$(prom_metric "$STREAM_URL" stream_events_consumed_total)"
  c_ch="$(ch_count)"
  d_prod=$(( (c_prod - p_prod) / SAMPLE ))
  d_cons=$(( (c_cons - p_cons) / SAMPLE ))
  d_ch=$(( (c_ch - p_ch) / SAMPLE ))
  elapsed=$(( $(now_epoch) - t0 ))
  printf '%ds\t%d\t%d\t%d\t%s\n' "$elapsed" "$d_prod" "$d_cons" "$d_ch" "$(send_errors)" | tee -a "$out"
  p_prod=$c_prod; p_cons=$c_cons; p_ch=$c_ch
  sum_prod=$(( sum_prod + d_prod )); sum_cons=$(( sum_cons + d_cons )); sum_ch=$(( sum_ch + d_ch ))
  samples=$(( samples + 1 ))
done

echo "# sustained means over ${samples} samples:"
printf 'produced=%d eps  consumed=%d eps  ingested=%d eps\n' \
  $(( sum_prod / samples )) $(( sum_cons / samples )) $(( sum_ch / samples ))
echo "# consumer lag at end of run:"
"$(dirname "$0")/capture-lag.sh" || echo "(lag snapshot skipped)"

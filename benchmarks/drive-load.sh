#!/usr/bin/env bash
# Drive a load profile against the running generator and print the effective
# end-to-end throughput (produced delta / sample interval) as it runs.
#
#   ./drive-load.sh steady <eps> <durationSec>
#   ./drive-load.sh ramp   <from> <to> <durationSec>
#   ./drive-load.sh spike  <baseEps> <peakEps> <spikeSec>
#
# The ramp/burst themselves run server-side in the generator; this script only
# kicks them off and samples throughput. Override the sample cadence with
# SAMPLE_SECONDS (default 5). Requires the stack to be up (docker compose up).
set -euo pipefail
source "$(dirname "$0")/lib.sh"
require curl

SAMPLE_SECONDS="${SAMPLE_SECONDS:-5}"

usage() {
  sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

# Sample effective eps every SAMPLE_SECONDS for the given total duration.
sample_loop() {
  local total="$1" prev now delta eff elapsed t0
  prev="$(gen_produced)"; t0="$(now_epoch)"; elapsed=0
  printf '%-9s %-14s %-10s %-9s\n' "elapsed" "produced" "eff_eps" "target"
  while [ "$elapsed" -lt "$total" ]; do
    sleep "$SAMPLE_SECONDS"
    now="$(gen_produced)"
    delta=$(( now - prev ))
    eff=$(( delta / SAMPLE_SECONDS ))
    elapsed=$(( $(now_epoch) - t0 ))
    printf '%-9s %-14s %-10s %-9s\n' "${elapsed}s" "$now" "$eff" "$(gen_target)"
    prev="$now"
  done
}

cmd="${1:-}"; shift || true
case "$cmd" in
  steady)
    [ "$#" -eq 2 ] || usage 1
    echo "# steady $1 eps for $2s"
    gen_rate "$1"; sample_loop "$2"
    ;;
  ramp)
    [ "$#" -eq 3 ] || usage 1
    echo "# ramp $1 → $2 eps over $3s"
    gen_ramp "$1" "$2" "$3"; sample_loop "$3"
    ;;
  spike)
    [ "$#" -eq 3 ] || usage 1
    echo "# baseline $1 eps, spike to $2 eps for $3s"
    gen_rate "$1"; sleep 3
    gen_burst "$2" "$3"; sample_loop $(( $3 + 6 ))
    ;;
  -h|--help|"")
    usage 0
    ;;
  *)
    echo "unknown profile: $cmd" >&2; usage 1
    ;;
esac

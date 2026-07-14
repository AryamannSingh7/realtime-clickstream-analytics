# Benchmarks

Load-generation harness and capture scripts for the clickstream pipeline. Results and
methodology are written up in [`../docs/benchmarks.md`](../docs/benchmarks.md).

## Prerequisites

- The full stack running: `docker compose up -d` (wait ~1–2 min for data to flow).
- `curl` on your `PATH`. (No `jq` needed — responses are parsed with `sed`/`awk`.)

Endpoints default to the local compose stack and can be overridden with env vars:
`GENERATOR_URL` (`:8089`), `STREAM_URL` (`:8090`), `ANALYTICS_API_URL` (`:8091`),
`CLICKHOUSE_URL` (`:8123`).

## Load profiles — `drive-load.sh`

Kicks off a load shape and prints the effective end-to-end throughput as it runs:

```bash
./drive-load.sh steady 5000 60       # hold 5 000 eps for 60 s
./drive-load.sh ramp   200 20000 120 # linearly ramp 200 → 20 000 eps over 120 s
./drive-load.sh spike  200 3000 30   # 200 eps baseline, spike to 3 000 eps for 30 s
```

The ramp/burst run **server-side** in the generator (`POST /api/generator/ramp|burst`),
so they keep their shape even if this script is interrupted. The generator now spreads
load across `GENERATOR_EMITTER_THREADS` parallel producers (default 4) — raise it to push
the rate higher on a multi-core box.

## `lib.sh`

Sourced helpers shared by every bench script: generator control (`gen_rate`, `gen_ramp`,
`gen_burst`, `gen_produced`), Prometheus metric reads (`prom_metric <base> <name>`), and
ClickHouse HTTP queries (`ch_query "<sql>"`).

## Roadmap

This directory is built up across M7:

- **M7.1 (done)** — load harness: server-side ramp/burst profiles + `drive-load.sh`.
- **M7.2 (done)** — `capture-throughput.sh`, `capture-latency.sh`, `capture-lag.sh`;
  results in [`../docs/benchmarks.md`](../docs/benchmarks.md).
- **M7.3** — ClickHouse query latency (`windowFunnel`/aggregation) over 10M+ rows.
- **M7.4** — horizontal scaling: throughput vs 1/2/3 Streams instances + rebalancing.

Captured results land under `results/` (git-ignored) and are summarised in
`docs/benchmarks.md` with a hardware disclosure.

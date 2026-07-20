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

## Capture scripts

All of M7 is complete; every figure below is written up with methodology and a hardware
disclosure in [`../docs/benchmarks.md`](../docs/benchmarks.md).

| Script | Measures |
|---|---|
| `capture-throughput.sh <eps> <dur>` | Sustained rate at three stages: produced → consumed → ingested |
| `capture-latency.sh <dur>` | Ingest-path latency (`ingested_at − event_time`) percentiles |
| `capture-lag.sh` | Consumer-group backlog for the Streams app and the ClickHouse sink |
| `capture-query-latency.sh <rows> <runs>` | ClickHouse OLAP engine time (cold vs warm) over an N-row dataset |
| `capture-scaling.sh <eps> <dur>` | Aggregate group throughput vs 1/2/3 stream-processor instances |

Scaling runs need the scale overlay, which clears the fixed container name and host port so
replicas can join the same consumer group:

```bash
docker compose -f docker-compose.yml -f docker-compose.scale.yml \
    up -d --no-deps --scale stream-processor=3 stream-processor
./capture-scaling.sh 30000 30
```

Captured results land under `results/` (git-ignored) and are summarised in
`docs/benchmarks.md`.

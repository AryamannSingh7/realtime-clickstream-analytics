# Benchmarks

Measured performance of the clickstream pipeline under synthetic load. Numbers are
reproducible with the scripts in [`../benchmarks/`](../benchmarks); every figure below
was captured on the hardware disclosed here — treat them as *this laptop's* numbers,
not universal ceilings.

## Hardware & configuration

| | |
|---|---|
| CPU | Intel Core i5-14450HX — 10 cores / 16 threads |
| RAM | 24 GB (Docker/WSL2 backend gets ~50% ≈ 12 GB by default) |
| OS | Windows 11 Home 26200, Docker Engine 29.4.1 (WSL2 backend) |
| Stack | Single-node `docker compose` — 1 Kafka broker (KRaft), 1 Schema Registry, 1 Kafka Connect, 1 stream-processor (2 stream threads, EOS-v2), 1 ClickHouse, 1 analytics-api |
| Generator | `event-generator`, 4 emitter threads, 500 virtual users, 5% late events (backdated ≤ 60s) |

Everything runs on one machine, so the generator, the brokers, the stream processor and
ClickHouse **compete for the same 16 threads** — the throughput ceiling below reflects
that contention, not a distributed deployment.

## Methodology

- **Throughput** — `capture-throughput.sh <eps> <dur>` sets a steady target rate and
  samples the effective rate at three pipeline stages every 5s: produced (generator
  `totalProduced`), consumed (stream-processor `stream.events.consumed`), ingested
  (ClickHouse `count()`). The sustained end-to-end rate is the ClickHouse ingest rate.
- **Latency (produced → visible)** — `capture-latency.sh`. The ingest path is measured
  directly in ClickHouse as `ingested_at − event_time` (the table stamps
  `ingested_at DEFAULT now64(3)` on insert). Reported over the on-time population
  (`< 2000ms`) to exclude the injected 5% late events, which are deliberately backdated
  up to 60s and would otherwise distort the tail.
- **Consumer lag** — `capture-lag.sh` sums `LAG` across all partitions for the Streams
  app group and the ClickHouse sink group, straight from `kafka-consumer-groups`.

Two latencies are reported **separately and deliberately**:
1. **Ingest / non-windowed path** (produced → queryable in ClickHouse) — the number that
   should be sub-second.
2. **Windowed / suppressed rollups** (`analytics.metrics.1m`, top-N pages) — these become
   visible **≈ one window length (60s) + grace after event time by design**. Kafka Streams
   suppression holds a window until it closes so each window emits exactly once; that delay
   is a correctness feature, not lag, and is not counted against the ingest latency.

## Results

_Captured 2026-07-14 on the hardware above. Each rate held for 30s, sampled every 5s;
figures are the mean of the samples._

### Throughput

| target eps | produced | consumed | ingested (sustained) | send errors | notes |
|---|---|---|---|---|---|
| 5,000 | 5,500 | 5,508 | 5,460 | 0 | pipeline keeps up — ingest ≈ production |
| 20,000 | 21,887 | 18,506 | 20,533 | 0 | near the ceiling; stream-processor begins to fall behind |
| 50,000 | 54,956 | 17,988 | 18,716 | 0 | generator scales, pipeline saturates ≈ 18k, lag grows unbounded |

**Headline:** the **generator sustains 55k eps** (4 threads, zero send errors), but the
**end-to-end pipeline tops out at ≈ 18–20k eps** on this single-node, 16-thread laptop.
Above that the generator outruns the pipeline and the backlog grows without bound — the
ceiling is the **stateful, exactly-once stream-processor and the ClickHouse sink competing
for the same cores**, not ingestion or the producer. This is the honest number: well short
of the aspirational 50–100k (which assumes a distributed cluster), and the bottleneck is
exactly where a stateful EOS topology should be.

### End-to-end latency (ingest path, produced → visible)

| load | samples | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| 200 eps (baseline) | 22,815 | 230ms | 418ms | 1,268ms | ~2,000ms |
| 500 eps (steady state, sink caught up) | 14,207 | 201ms | 301ms | 729ms | 1,815ms |

p95 stays **sub-second** (301–418ms) while the pipeline is within its sustainable envelope —
meeting the target. (The windowed rollups are, by design, ~60s behind — see Methodology.)

### Consumer lag

Lag is reported for the **ClickHouse sink** group, which cleanly tracks the ingest path:

| scenario | sink lag |
|---|---|
| ≤ 5,000 eps (steady state) | < 8,000 — bounded, tracks the sink's batch cadence |
| 50,000 eps, 30s (over-ceiling) | 1,628,224 and climbing |

Within the ceiling, arrival ≈ ingest (see the throughput table: at 500 and 5,000 eps
produced ≈ consumed ≈ ingested) so lag stays flat. Above it, the sink accumulates backlog
without bound — the signature of a saturated pipeline.

**Recovery is slow after a deliberate overload.** The 50k run buffered millions of records in
the source and internal repartition topics; the stateful EOS stream-processor then took
several minutes to re-process that backlog (its group lag peaked ~10M during catch-up) even
after the arrival rate dropped back to 500 eps — while the sink, an independent consumer
group, had already drained to < 8,000. That asymmetry is expected: rebuilding suppressed
windows and repartitioned state is inherently heavier than a stateless row insert.

### OLAP query latency (10M rows)

_Captured 2026-07-17. Dataset: **10,000,000** synthetic events, **~1.0M distinct visitors**,
213 MiB on disk (`analytics.events_bench`). Latency is ClickHouse **engine time**
(`query_duration_ms` from `system.query_log`, so HTTP/JDBC overhead is excluded), memory
capped at 4 GB/query. Cold = mark + uncompressed caches dropped; warm = median of 5 runs._

| query (the analytics-api OLAP endpoint it backs) | rows scanned | cold | warm (median) | warm min–max |
|---|---|---|---|---|
| funnel — `windowFunnel`, GROUP BY visitor | 10.0M | 794 ms | **708 ms** | 616–740 ms |
| unique visitors — `uniqCombined`, hourly | 10.0M | 85 ms | **85 ms** | 80–125 ms |
| event timeseries — count by hour + type | 10.0M | 47 ms | **46 ms** | 42–48 ms |
| top pages — `page_view` count by path | 4.1M | 18 ms | **18 ms** | 17–19 ms |
| retention — `retention()`, two buckets | 10.0M | 351 ms | **325 ms** | 317–371 ms |

Every query the dashboard's OLAP side serves stays **sub-second over 10M rows** on this laptop.
`windowFunnel` is the heaviest (~0.7s) because it groups every one of ~1M visitors and walks their
event sequence; the plain aggregations land in tens of milliseconds. Top-pages scans only 4.1M rows
(not 10M) because the table's `ORDER BY (event_type, event_time, anonymous_id)` lets ClickHouse prune
straight to the `page_view` rows. Cold vs warm barely differ — these are full-column scans, so the OS
page cache dominates and ClickHouse's own caches add little. (These are the ad-hoc OLAP path; the live
dashboard tiles are served from the streaming SSE side, never from a 10M-row scan.)

### Horizontal scaling (stream-processor, 1 / 2 / 3 instances)

_Captured 2026-07-17. The stream-processor is scaled with `docker-compose.scale.yml` (which
clears the fixed container name + host port so replicas can join the same
`application.id = clickstream-stream-processor` group). Load is driven deliberately over the
ceiling at 30k eps; `capture-scaling.sh` reports the group's aggregate source-consumption rate
(committed-offset delta on `clickstream.events.raw` over a 30s window) after warmup._

| instances | sustained consume (eps) | source-partition spread |
|---|---|---|
| 1 | ~20,000 | all 6 partitions on the one instance |
| 2 | ~15,700 | 6 kept together (warm stateful tasks not moved) |
| 3 | ~16,500 | 5 + 1 across two instances |

**Finding: on a single machine, adding stream-processor instances does not raise throughput** —
it stays pinned at the **~15–20k core-bound ceiling** from the throughput section. Every replica
competes for the same 16 threads, so there is no extra compute to exploit; the
rebalance / state-restore / coordination overhead only adds variance (and can transiently *lower*
throughput while a fresh instance restores its RocksDB state under load — the lag kept growing in
all three runs because 30k is over the ceiling). The mechanism itself works: the consumer group
grows to N members and Kafka Streams redistributes work, with source partitions spreading across
instances (5 + 1 at three instances). It balances whole **tasks**, not raw partitions, and its
high-availability assignor keeps warm stateful tasks put rather than pay a restore cost — so on one
box the six stateful source tasks tend to stay co-located. The topology is horizontally scalable by
construction (shared-nothing, coordinated purely through the Kafka consumer group under EOS-v2);
realizing an actual throughput *gain* needs **more nodes**, which a single laptop cannot provide.
This is the same conclusion as the throughput ceiling: the bound is stateful EOS processing plus the
sink competing for cores, not the code.

## Reproducing

```bash
docker compose up -d            # wait ~1–2 min for data to flow
cd benchmarks
./capture-throughput.sh 10000 60      # sustained-rate sweep
./capture-latency.sh 60               # ingest-path latency
./capture-lag.sh                      # consumer-group backlog
./capture-query-latency.sh 10000000 5 # OLAP query latency over 10M rows

# Horizontal scaling: scale the stream-processor, then measure the group throughput.
docker compose -f docker-compose.yml -f docker-compose.scale.yml \
    up -d --no-deps --scale stream-processor=3 stream-processor
./capture-scaling.sh 30000 30
```

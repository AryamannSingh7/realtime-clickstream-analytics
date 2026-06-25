# Architecture

> Living document — expanded as milestones land. See [`decisions/`](decisions/) for the
> Architecture Decision Records (ADRs) behind the key choices.

## Goal

Ingest a high-volume clickstream, process it with **stateful, event-time stream
operations**, store results in a **columnar OLAP store**, and surface them on a **live
dashboard** — all free, all containerized, all runnable with one command.

## High-level data flow

```
event-generator ──▶ Kafka ──┬──▶ ClickHouse  (raw events; OLAP: funnel / unique / retention)
  (realistic        (Avro,  │     via Kafka Connect Sink (exactly-once)
   user journeys)    Schema  │
                     Registry)
                            └──▶ Kafka Streams ──▶ analytics.* topics ──▶ analytics-api ──▶ Next.js
                                 (event-time windows,                     (SSE live feed +    dashboard
                                  sessionization, top-N,                   ClickHouse OLAP)
                                  funnel, anomaly; EOS)
```

## Component responsibilities

| Component | Role |
|---|---|
| **event-generator** | Synthesizes realistic e-commerce journeys (weighted state machine); configurable EPS, bursts, late events. Produces Avro to `clickstream.events.raw`. |
| **Kafka (KRaft)** | Durable, replayable event transport. |
| **Schema Registry** | Avro schema enforcement + evolution. |
| **ClickHouse** | Columnar store for raw events; serves heavy OLAP (funnel, unique visitors, retention, time-series). |
| **Kafka Connect (ClickHouse Sink)** | Exactly-once ingestion Kafka → ClickHouse; decouples ingest from query load. |
| **stream-processor (Kafka Streams)** | Low-latency streaming rollups: sessionization, per-minute metrics, windowed top-N, live funnel, anomaly detection. EOS + Interactive Queries. |
| **analytics-api (Spring Boot)** | Fans out `analytics.*` topics to the UI over SSE; exposes ClickHouse-backed OLAP endpoints. |
| **dashboard (Next.js)** | Live-updating analytics views. |

## Architectural through-line

**Kafka Streams owns low-latency streaming rollups; ClickHouse owns heavy OLAP.** Each
tool does what it is best at — a deliberate split, documented in
[ADR-0002](decisions/0002-storage-clickhouse.md).

## Topics

| Topic | Purpose | Notes |
|---|---|---|
| `clickstream.events.raw` | Source event stream | Avro, keyed by `anonymous_id` (per-user ordering for sessionization) |
| `ref.products`, `ref.campaigns` | Reference data | Compacted, KTable sources for enrichment joins |
| `analytics.sessions` | Completed session summaries | From session windows |
| `analytics.metrics.1m` | Per-minute rollups | Tumbling windows |
| `analytics.topn.pages.1m` | Live top-N pages | Windowed top-N |
| `analytics.funnel.live` | Live funnel progress | Stateful |
| `analytics.alerts` | Anomaly / spike alerts | Stateful EWMA |

## Storage schema

See [`infra/clickhouse/init/01-schema.sql`](../infra/clickhouse/init/01-schema.sql).
`analytics.events` is a `MergeTree` partitioned by day, ordered by
`(event_type, event_time, anonymous_id)`.

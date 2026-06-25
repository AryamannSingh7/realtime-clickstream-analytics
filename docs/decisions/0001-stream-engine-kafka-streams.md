# ADR-0001: Kafka Streams as the stream-processing engine

**Status:** Accepted · **Date:** 2026-06-25

## Context

The pipeline needs a stateful, event-time stream-processing engine for sessionization,
windowed aggregations, top-N, funnels, and anomaly detection. The two realistic
candidates are **Apache Flink** and **Kafka Streams**.

## Decision

Use **Kafka Streams** (Java 17).

## Rationale

- **Depth over breadth.** The project deliberately deepens the Kafka ecosystem rather
  than spreading across a second runtime. Kafka Streams is a genuinely distinct, deep
  skill from "using Kafka as a queue": KStream/KTable duality, state stores backed by
  RocksDB, exactly-once-v2 (EOS), suppression for windowed final results, and Interactive
  Queries.
- **The depth is in the topology, not the engine badge.** Sessionization (session
  windows), windowed top-N, stateful funnels, and EWMA-based anomaly detection exercise
  the full stream-processing toolbox regardless of engine.
- **Operational simplicity.** No separate cluster/job-manager to run; the processor is a
  plain JVM app, trivially containerized and scaled by adding instances (consumer-group
  rebalancing).
- **Testability.** `TopologyTestDriver` gives fast, deterministic unit tests of windowing
  and stateful logic without a running broker.

## Trade-off (honest)

Flink appears more often as a standalone keyword in job descriptions and has richer
native CEP / watermark controls. We accept this: for *this* stack and goal, a coherent,
deep Kafka-ecosystem story is more valuable than a second shallow runtime. A Flink port
could be a future exploration.

## Consequences

- Output rollups are written to `analytics.*` topics and consumed by the API.
- Heavy OLAP (funnel, retention, unique counts) is delegated to ClickHouse — see
  [ADR-0002](0002-storage-clickhouse.md).

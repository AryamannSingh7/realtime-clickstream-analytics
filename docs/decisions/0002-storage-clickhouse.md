# ADR-0002: ClickHouse as the analytics store

**Status:** Accepted · **Date:** 2026-06-25

## Context

We need a store for raw events and ad-hoc analytical queries (conversion funnels, unique
visitors, retention, time-series over large volumes). Candidates: **ClickHouse** vs
**TimescaleDB**.

## Decision

Use **ClickHouse**.

## Rationale

- **Built for clickstream.** ClickHouse's name literally derives from "Clickstream +
  Data Warehouse"; it was designed for exactly this workload.
- **Native analytical functions** that map directly onto our use case:
  `windowFunnel()` (conversion funnels), `uniqCombined()` (HyperLogLog unique visitors),
  `retention()` (cohorts).
- **Columnar + vectorized** execution gives sub-second aggregations over tens of millions
  of rows, and very high ingest throughput.
- **Free & open-source** (Apache 2.0), self-hostable in a single container.

## Trade-off (honest)

TimescaleDB is the better fit for narrow, Postgres-compatible time-series workloads and
teams that want SQL/ORM familiarity. For high-cardinality OLAP and funnel/retention
analytics, ClickHouse is materially stronger, which is what this project showcases.

## Consequences

- Nested Avro (`device.*`, `geo.*`) is flattened to `*_` columns on ingest (Connect
  Flatten SMT) — see [`01-schema.sql`](../../infra/clickhouse/init/01-schema.sql).
- The streaming engine (Kafka Streams) handles low-latency rollups; ClickHouse handles
  heavy OLAP. See [ADR-0003](0003-clickhouse-ingestion-kafka-connect.md) for ingestion.

# ADR-0003: Kafka → ClickHouse ingestion via the official Kafka Connect Sink

**Status:** Accepted · **Date:** 2026-06-25

## Context

Raw events on `clickstream.events.raw` must land in ClickHouse. Two common approaches:
the **ClickHouse Kafka table engine** (ClickHouse consumes Kafka directly) vs the
**official ClickHouse Kafka Connect Sink** (a Connect worker pushes into ClickHouse).

## Decision

Use the **official ClickHouse Kafka Connect Sink**.

## Rationale

- **Exactly-once delivery** support (offsets tracked reliably) — pairs with Kafka Streams
  EOS for an end-to-end exactly-once story.
- **Decouples ingest load from query load**: ingestion runs in the Connect worker, not on
  the ClickHouse query nodes.
- **Per-event transforms** via SMTs (e.g. `Flatten` to map nested Avro `device.*`/`geo.*`
  to flat `*_` columns; field selection).
- Demonstrates the broader **Kafka Connect** ecosystem — consistent with the
  "deepen the Kafka ecosystem" theme of [ADR-0001](0001-stream-engine-kafka-streams.md).

## Trade-off (honest)

The Kafka table engine is simpler (no extra component) but provides only at-least-once
semantics and couples ingest load to the ClickHouse cluster. The added Connect worker is
worth it for exactly-once and load isolation. Noted as the lighter alternative.

## Consequences

- A `kafka-connect` service and the ClickHouse sink connector are added in M1.
- Connector config lives under `infra/kafka-connect/`.

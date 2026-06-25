# Real-Time Clickstream Analytics Pipeline

[![CI](https://github.com/AryamannSingh7/realtime-clickstream-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/AryamannSingh7/realtime-clickstream-analytics/actions/workflows/ci.yml)

A production-minded, end-to-end **real-time analytics pipeline** that ingests a high-volume clickstream, processes it with **stateful, windowed stream operations**, stores results in a **columnar OLAP store**, and surfaces them on a **live dashboard** — all containerized and runnable with a single command.

> **Status:** 🚧 Under active construction. Built milestone-by-milestone; see the [roadmap](#roadmap) below.

---

## Why this project

Most "analytics" demos are batch jobs in disguise. This one is genuinely **streaming**: events are aggregated *as they arrive* using event-time windows, sessionization, and stateful pattern detection — the same techniques that power real product-analytics platforms (think Mixpanel / Google Analytics, in real time).

It's built to demonstrate **data-engineering and stream-processing depth**:

- **Event-time processing** with watermarks and late-event handling
- **Sessionization** via session windows (30-minute inactivity gap)
- **Windowed aggregations** — per-minute rollups and live top-N
- **Stateful processing** — real-time conversion funnels and anomaly/spike detection
- **Stream–table joins** for event enrichment
- **Exactly-once semantics** end-to-end
- A clean separation of concerns: **stream processing for low-latency rollups, columnar OLAP for heavy funnel/retention queries**

## Architecture (at a glance)

```
event-generator ──▶ Kafka ──┬──▶ ClickHouse (raw events, OLAP: funnel / unique / retention)
  (realistic        (Avro)  │
   user journeys)           └──▶ Kafka Streams ──▶ analytics topics ──▶ analytics-api ──▶ Next.js
                                 (sessions, metrics,                     (SSE live feed +    dashboard
                                  top-N, funnel, alerts)                  OLAP queries)
```

Full architecture write-up and decision records will live in [`docs/`](docs/).

## Tech stack

| Layer | Technology | Why |
|---|---|---|
| Transport | **Apache Kafka** (KRaft) | High-throughput, replayable event log |
| Schema | **Avro + Schema Registry** | Schema enforcement & evolution |
| Stream processing | **Kafka Streams** (Java 17) | Stateful, event-time streaming with exactly-once + Interactive Queries |
| Storage | **ClickHouse** | Columnar OLAP built for clickstream; native `windowFunnel` / `uniqCombined` |
| Serving | **Spring Boot** (SSE + ClickHouse queries) | Live push to the UI + OLAP API |
| Dashboard | **Next.js + React** | Live-updating analytics views |
| Infra | **Docker Compose** | One-command, fully reproducible local stack |
| CI | **GitHub Actions** | Build, unit + integration tests (Testcontainers) |

Everything in the stack is **free and open-source** — no paid services required to run it.

## Roadmap

- [x] **M0** — Scaffolding & infra (Kafka, Schema Registry, ClickHouse via Docker Compose) ✅
- [ ] **M1** — Ingestion: realistic event generator → Kafka → ClickHouse
- [ ] **M2** — Streaming core: per-minute metrics + windowed top-N
- [ ] **M3** — Sessionization + conversion funnel + enrichment joins
- [ ] **M4** — Stateful anomaly / spike detection
- [ ] **M5** — Serving API (SSE live feeds + ClickHouse OLAP endpoints)
- [ ] **M6** — Live Next.js dashboard
- [ ] **M7** — Load harness & benchmarks (throughput / latency / lag)
- [ ] **M8** — Polish: docs, architecture diagram, tests, CI badge, demo recording

## Getting started

**Prerequisites:** Docker Desktop, JDK 17. (Maven not required — a wrapper is included.)

```bash
# 1. Bring up the infra (Kafka KRaft + Schema Registry + ClickHouse) and create topics
docker compose up -d

# 2. Verify everything is wired correctly
bash scripts/smoke-test.sh

# 3. Build the Java services (Avro codegen + compile + tests)
./mvnw -B verify
```

| Service | Endpoint |
|---|---|
| Kafka (host) | `localhost:9092` |
| Schema Registry | http://localhost:8081 |
| ClickHouse (HTTP) | http://localhost:8123 — user `clickstream` / pass `clickstream`, db `analytics` (local dev creds) |

Tear down with `docker compose down` (add `-v` to also drop data volumes).

## Project layout

```
schemas/            # Shared Avro schema(s) — source of truth for event models
services/
  common/           # Avro-generated event models (shared module)
infra/clickhouse/    # ClickHouse init SQL (schema)
scripts/            # Helper scripts (smoke test, etc.)
docs/               # Architecture write-up + ADRs (decision records)
docker-compose.yml  # One-command local stack
```

---

*Built as a portfolio project demonstrating real-time data-engineering and stream-processing.*

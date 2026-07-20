# ADR-0004: Testcontainers integration tests alongside TopologyTestDriver

**Status:** Accepted · **Date:** 2026-07-17

## Context

Until M8 the suite was entirely broker-less: Kafka Streams topologies were tested with
`TopologyTestDriver`, and `OlapService` was tested with a mocked `JdbcTemplate`. Both are
fast and deterministic, and they cover the logic well — but they share a blind spot. **They
never exercise the thing the code actually talks to.**

Concretely, the broker-less layer cannot catch:

- **Avro serialization over a real Schema Registry.** `TopologyTestDriver` is handed serdes
  directly; schema registration, subject naming and compatibility never happen.
- **Exactly-once (EOS-v2) across a real broker.** The driver processes records
  synchronously in-process; there are no transactions, no commits, no consumer group.
- **Wall-clock punctuators.** The live funnel and active-sessions snapshots emit on a
  wall-clock cadence. The driver advances time only when told to, so the real timing path
  is untested.
- **SQL that is wrong against a real server.** A mocked `JdbcTemplate` returns whatever the
  test says it returns. `windowFunnel` / `uniqCombined` syntax errors, `DateTime64` timezone
  mismatches and schema drift all pass silently.

That last category is the dangerous one: a mocked query test can be green while the
endpoint is broken in production.

## Decision

Add a second, **opt-in** test layer using **Testcontainers** against real infrastructure,
and keep the broker-less layer as-is.

- `StreamProcessorIT` — real Kafka + Schema Registry. Assembles the topology from the *same
  production builders* the app wires (`SourceStreamTopology.source` + `LiveFunnelTopology.build`),
  runs it under EOS-v2, produces real Avro journeys and awaits the converged funnel snapshot.
- `OlapIntegrationIT` — real ClickHouse. Applies the **checked-in production DDL verbatim**
  (`infra/clickhouse/init/01-schema.sql`) so the fixture cannot drift from the real schema,
  then asserts the funnel / unique-visitor / top-pages / timeseries queries over a known
  fixture.

Integration tests live in `*IT.java`, run under **Failsafe** at `verify`, and are gated
behind the `integration-tests` Maven profile. Surefire keeps running `*Test.java` on every
build. CI runs them as a **separate job** from the fast build.

## Rationale

The two layers are complementary, not redundant. Unit tests give fast feedback on topology
logic and aggregation math; integration tests prove the wiring against real Kafka, real
Schema Registry and real ClickHouse. Gating behind a profile keeps `./mvnw verify` fast and
Docker-free for everyday work while CI still gets the real coverage on every push.

Assembling the IT topology from the production builders (rather than restating it in the
test) is what makes the test meaningful — a test that builds its own topology only proves
the test's topology works.

## Trade-off (honest)

Integration tests are **slower** (container startup dominates: pulling and booting Kafka,
Schema Registry and ClickHouse) and are a **Docker dependency**. They are also more prone to
timing flakiness, which is why the funnel assertion uses Awaitility with a generous 90s
budget rather than a fixed sleep. The profile split is the mitigation: nobody pays that cost
unless they ask for it or they are CI.

## Consequences

Three gotchas surfaced while wiring this up, each with a fix worth remembering:

- **Failsafe vs. the Spring Boot fat jar.** `spring-boot:repackage` runs at `package` and
  rewrites `<module>.jar` with classes under `BOOT-INF/classes/`. Failsafe defaults to
  testing the module's *artifact*, so the app's own classes become invisible
  (`NoClassDefFoundError`). Fixed by pointing Failsafe at the exploded classes:
  `<classesDirectory>${project.build.outputDirectory}</classesDirectory>`.
- **ClickHouse TTL vs. fixture data.** `analytics.events` carries `TTL event_time + 30 DAY`.
  A fixture at a fixed past date is swept immediately and the test sees an empty table. The
  fixture is therefore anchored to the *start of the current hour*, which also keeps every
  event in one hourly bucket for clean assertions. Rows are inserted with
  `fromUnixTimestamp64Milli(...)` — the exact form the queries bound on — so insert and query
  share identical epoch semantics with no `DateTime64` timezone ambiguity.
- **docker-java on local Windows.** Running the ITs on a Windows host needed an
  `api.version` workaround for docker-java to negotiate with Docker Desktop. CI
  (`ubuntu-latest`, which ships a running Docker engine) is unaffected.

Schema Registry is run as a plain `GenericContainer` on a shared `Network` with an explicit
internal listener (`kafka:19092`), because it must reach the broker container-to-container
while the test client reaches it on the mapped host port.

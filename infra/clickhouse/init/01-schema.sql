-- ClickHouse schema for the clickstream analytics pipeline.
-- Auto-applied on first container start (mounted into /docker-entrypoint-initdb.d).
--
-- Design notes:
--  * Raw events land here from Kafka via the ClickHouse Kafka Connect Sink (added in M1).
--  * Nested Avro records (device.*, geo.*) are flattened to *_ columns by a Connect
--    Flatten SMT (delimiter "_"), so column names match below.
--  * ORDER BY is tuned for the two dominant access patterns: funnel/event-type scans
--    and time-range queries.

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events
(
    event_id        String,
    event_type      LowCardinality(String),
    anonymous_id    String,
    user_id         Nullable(String),
    event_time      DateTime64(3),
    path            String,
    referrer        Nullable(String),
    utm_source      LowCardinality(Nullable(String)),
    utm_campaign    Nullable(String),
    product_id      Nullable(String),
    price           Nullable(Float64),
    revenue         Nullable(Float64),
    device_type     LowCardinality(String),
    device_os       LowCardinality(String),
    device_browser  LowCardinality(String),
    geo_country     LowCardinality(String),
    geo_city        String,
    user_agent      Nullable(String),
    ingested_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (event_type, event_time, anonymous_id)
TTL toDateTime(event_time) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

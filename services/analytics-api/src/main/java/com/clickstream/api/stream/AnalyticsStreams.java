package com.clickstream.api.stream;

/**
 * Source Kafka topic names and the SSE stream keys they fan out to. Kept in one place so the
 * {@code @KafkaListener} topics and the {@code /api/stream/*} paths can't drift apart.
 */
final class AnalyticsStreams {

    private AnalyticsStreams() {
    }

    // --- Kafka output topics (produced by the stream-processor) ---
    static final String METRICS_TOPIC = "analytics.metrics.1m";
    static final String TOPN_TOPIC = "analytics.topn.pages.1m";
    static final String FUNNEL_TOPIC = "analytics.funnel.live";
    static final String ALERTS_TOPIC = "analytics.alerts";
    static final String ACTIVE_SESSIONS_TOPIC = "analytics.active.sessions";

    // --- SSE stream keys (also the /api/stream/{key} path segment and the SSE event name) ---
    static final String METRICS = "metrics";
    static final String TOPN = "topn";
    static final String FUNNEL = "funnel";
    static final String ALERTS = "alerts";
    static final String ACTIVE_SESSIONS = "active-sessions";
}

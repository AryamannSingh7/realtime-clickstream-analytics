package com.clickstream.processor.topology;

import java.time.Duration;

/**
 * Central catalogue of topic names and windowing constants shared across the topology
 * and its tests, so the wiring and the assertions can never drift apart.
 */
public final class StreamTopics {

    private StreamTopics() {
    }

    /** Raw ingress topic — Avro values, keyed by anonymous_id for per-user ordering. */
    public static final String RAW_EVENTS = "clickstream.events.raw";

    /** Per-visitor session summaries ({@code SessionSummary}), one record per closed session. */
    public static final String SESSIONS = "analytics.sessions";

    /** Per-minute whole-stream rollup ({@code MinuteMetrics}), one record per closed window. */
    public static final String METRICS_1M = "analytics.metrics.1m";

    /** Top-N most-viewed page paths per minute ({@code PageTopN}), one record per closed window. */
    public static final String TOPN_PAGES_1M = "analytics.topn.pages.1m";

    /** Size of the tumbling event-time window every rollup is computed over. */
    public static final Duration WINDOW_SIZE = Duration.ofMinutes(1);

    /** Lateness allowance before a window is considered closed and emitted. */
    public static final Duration WINDOW_GRACE = Duration.ofSeconds(5);

    /** Inactivity gap that ends a session: 30 minutes of silence for a visitor closes it. */
    public static final Duration SESSION_INACTIVITY_GAP = Duration.ofMinutes(30);

    /** Lateness allowance before a closed session is emitted. */
    public static final Duration SESSION_GRACE = Duration.ofMinutes(1);
}

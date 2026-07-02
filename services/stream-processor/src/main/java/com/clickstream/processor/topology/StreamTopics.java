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

    /** Per-minute whole-stream rollup ({@code MinuteMetrics}), one record per closed window. */
    public static final String METRICS_1M = "analytics.metrics.1m";

    /** Top-N most-viewed page paths per minute ({@code PageTopN}), one record per closed window. */
    public static final String TOPN_PAGES_1M = "analytics.topn.pages.1m";

    /** Size of the tumbling event-time window every rollup is computed over. */
    public static final Duration WINDOW_SIZE = Duration.ofMinutes(1);

    /** Lateness allowance before a window is considered closed and emitted. */
    public static final Duration WINDOW_GRACE = Duration.ofSeconds(5);
}

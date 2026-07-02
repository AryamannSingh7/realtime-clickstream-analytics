package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.MinuteMetrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Instant;

/**
 * M2.3 — whole-stream per-minute rollup.
 *
 * <p>Collapses every {@link ClickEvent} onto a single key so all traffic lands in one
 * tumbling {@link TimeWindows event-time window}, aggregates event-type counts and purchase
 * revenue, and emits exactly one {@link MinuteMetrics} record per window once it closes
 * (via {@link Suppressed#untilWindowCloses}). Output goes to {@link StreamTopics#METRICS_1M}.
 */
public final class MinuteMetricsTopology {

    private MinuteMetricsTopology() {
    }

    /** Single grouping key — the rollup is global, not per-user or per-page. */
    private static final String GLOBAL_KEY = "all";

    public static void build(
            KStream<String, ClickEvent> source,
            Serde<ClickEvent> clickEventSerde,
            Serde<MinuteMetrics> minuteMetricsSerde) {

        Initializer<MinuteMetrics> initializer =
                () -> new MinuteMetrics(Instant.EPOCH, Instant.EPOCH, 0L, 0L, 0L, 0L, 0L, 0.0);

        Aggregator<String, ClickEvent, MinuteMetrics> aggregator = (key, event, agg) -> {
            agg.setEvents(agg.getEvents() + 1);
            switch (event.getEventType()) {
                case page_view -> agg.setPageViews(agg.getPageViews() + 1);
                case add_to_cart -> agg.setAddToCart(agg.getAddToCart() + 1);
                case checkout_start -> agg.setCheckoutStart(agg.getCheckoutStart() + 1);
                case purchase -> {
                    agg.setPurchases(agg.getPurchases() + 1);
                    if (event.getRevenue() != null) {
                        agg.setRevenue(agg.getRevenue() + event.getRevenue());
                    }
                }
                default -> {
                    // click / search / remove_from_cart / login / logout: counted only in `events`.
                }
            }
            return agg;
        };

        source
                .groupBy(
                        (key, event) -> GLOBAL_KEY,
                        Grouped.with("minute-metrics-group", Serdes.String(), clickEventSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(StreamTopics.WINDOW_SIZE, StreamTopics.WINDOW_GRACE))
                .aggregate(
                        initializer,
                        aggregator,
                        Materialized.with(Serdes.String(), minuteMetricsSerde))
                // One clean record per window, only after it can no longer change.
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedKey, metrics) -> {
                    // Stamp the window bounds now that we know them, and rekey by window start.
                    metrics.setWindowStart(Instant.ofEpochMilli(windowedKey.window().start()));
                    metrics.setWindowEnd(Instant.ofEpochMilli(windowedKey.window().end()));
                    return KeyValue.pair(String.valueOf(windowedKey.window().start()), metrics);
                }, Named.as("minute-metrics-finalize"))
                .to(StreamTopics.METRICS_1M, Produced.with(Serdes.String(), minuteMetricsSerde));
    }
}

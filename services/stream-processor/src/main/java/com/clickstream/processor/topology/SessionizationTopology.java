package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.SessionSummary;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;

import java.time.Instant;

/**
 * M3.1 — sessionization.
 *
 * <p>Groups every {@link ClickEvent} by its {@code anonymous_id} (already the stream key) into
 * event-time {@link SessionWindows session windows}: any run of activity separated by less than
 * {@link StreamTopics#SESSION_INACTIVITY_GAP} counts as one session, and a longer gap starts a
 * new one. Each session accumulates event-type counts, purchase revenue, its landing path, and
 * the first authenticated user id. A {@link Merger} folds together sub-sessions that a bridging
 * out-of-order event later joins. One {@link SessionSummary} per session is emitted to
 * {@link StreamTopics#SESSIONS} once the session closes (via {@link Suppressed#untilWindowCloses}).
 */
public final class SessionizationTopology {

    private SessionizationTopology() {
    }

    public static void build(
            KStream<String, ClickEvent> source,
            Serde<ClickEvent> clickEventSerde,
            Serde<SessionSummary> sessionSerde) {

        source
                .groupByKey(Grouped.with("sessionize-group", Serdes.String(), clickEventSerde))
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(
                        StreamTopics.SESSION_INACTIVITY_GAP, StreamTopics.SESSION_GRACE))
                .aggregate(initializer(), aggregator(), merger(),
                        Materialized.with(Serdes.String(), sessionSerde))
                // One clean summary per session, only after it can no longer change.
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedKey, session) -> {
                    // Stamp the identity and window bounds now that the session is final.
                    long startMs = windowedKey.window().start();
                    long endMs = windowedKey.window().end();
                    session.setAnonymousId(windowedKey.key());
                    session.setSessionStart(Instant.ofEpochMilli(startMs));
                    session.setSessionEnd(Instant.ofEpochMilli(endMs));
                    session.setDurationMs(endMs - startMs);
                    session.setConverted(session.getPurchases() > 0);
                    String sessionId = windowedKey.key() + ":" + startMs;
                    session.setSessionId(sessionId);
                    return KeyValue.pair(sessionId, session);
                }, Named.as("sessionize-finalize"))
                .to(StreamTopics.SESSIONS, Produced.with(Serdes.String(), sessionSerde));
    }

    private static Initializer<SessionSummary> initializer() {
        return () -> SessionSummary.newBuilder()
                .setSessionId("")
                .setAnonymousId("")
                .setUserId(null)
                .setSessionStart(Instant.EPOCH)
                .setSessionEnd(Instant.EPOCH)
                .setDurationMs(0L)
                .setEvents(0L)
                .setPageViews(0L)
                .setAddToCart(0L)
                .setCheckoutStart(0L)
                .setPurchases(0L)
                .setRevenue(0.0)
                .setEntryPath("")
                .setConverted(false)
                .build();
    }

    private static Aggregator<String, ClickEvent, SessionSummary> aggregator() {
        return (key, event, agg) -> {
            Instant eventTime = event.getEventTime();
            // Track the earliest event so entry_path is the true landing page, even out of order.
            if (agg.getEvents() == 0L || eventTime.isBefore(agg.getSessionStart())) {
                agg.setSessionStart(eventTime);
                agg.setEntryPath(event.getPath());
            }
            if (agg.getEvents() == 0L || eventTime.isAfter(agg.getSessionEnd())) {
                agg.setSessionEnd(eventTime);
            }
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
            if (agg.getUserId() == null && event.getUserId() != null) {
                agg.setUserId(event.getUserId());
            }
            return agg;
        };
    }

    /**
     * Folds session {@code b} into {@code a}; earliest start wins the landing path, counts and
     * revenue sum, first non-null user id sticks.
     *
     * <p>Session-window aggregation calls this on every record, folding each existing session into
     * a fresh {@link #initializer()} value before the {@link #aggregator()} runs — so {@code a} is
     * often the empty initializer, whose sentinel {@code session_start} of {@link Instant#EPOCH}
     * would otherwise falsely win the "earliest" comparison. An empty aggregate ({@code events==0})
     * therefore adopts {@code b}'s bounds outright; {@code b} is always a real stored session.
     */
    private static Merger<String, SessionSummary> merger() {
        return (key, a, b) -> {
            boolean aEmpty = a.getEvents() == 0L;
            if (aEmpty || b.getSessionStart().isBefore(a.getSessionStart())) {
                a.setSessionStart(b.getSessionStart());
                a.setEntryPath(b.getEntryPath());
            }
            if (aEmpty || b.getSessionEnd().isAfter(a.getSessionEnd())) {
                a.setSessionEnd(b.getSessionEnd());
            }
            a.setEvents(a.getEvents() + b.getEvents());
            a.setPageViews(a.getPageViews() + b.getPageViews());
            a.setAddToCart(a.getAddToCart() + b.getAddToCart());
            a.setCheckoutStart(a.getCheckoutStart() + b.getCheckoutStart());
            a.setPurchases(a.getPurchases() + b.getPurchases());
            a.setRevenue(a.getRevenue() + b.getRevenue());
            if (a.getUserId() == null && b.getUserId() != null) {
                a.setUserId(b.getUserId());
            }
            return a;
        };
    }
}

package com.clickstream.processor.topology;

import com.clickstream.avro.FunnelSnapshot;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

/**
 * M3.3 stage 2 — global funnel aggregator.
 *
 * <p>Receives per-visitor stage-advance deltas (all under one key, so this runs on a single
 * task) and keeps a cumulative distinct-visitor count for each funnel stage in a state store.
 * A wall-clock punctuator emits a {@link FunnelSnapshot} — the current counts plus step and
 * overall conversion rates — every {@link #SNAPSHOT_INTERVAL} to {@link StreamTopics#FUNNEL_LIVE},
 * giving the dashboard a steadily refreshing funnel regardless of event-time progression.
 */
public class FunnelSnapshotProcessor extends ContextualProcessor<String, Integer, String, FunnelSnapshot> {

    static final String STORE_NAME = "funnel-counters-store";
    static final String OUTPUT_KEY = "funnel";
    private static final Duration SNAPSHOT_INTERVAL = Duration.ofSeconds(3);

    private KeyValueStore<String, Long> counters;

    @Override
    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, FunnelSnapshot> context) {
        super.init(context);
        this.counters = context.getStateStore(STORE_NAME);
        context.schedule(SNAPSHOT_INTERVAL, PunctuationType.WALL_CLOCK_TIME, this::emitSnapshot);
    }

    @Override
    public void process(Record<String, Integer> record) {
        int stage = record.value();
        String key = String.valueOf(stage);
        long current = counters.get(key) == null ? 0L : counters.get(key);
        counters.put(key, current + 1);
    }

    private void emitSnapshot(long wallClockMs) {
        long views = count(FunnelStage.VIEW);
        long carts = count(FunnelStage.CART);
        long checkouts = count(FunnelStage.CHECKOUT);
        long purchases = count(FunnelStage.PURCHASE);

        FunnelSnapshot snapshot = new FunnelSnapshot(
                Instant.ofEpochMilli(wallClockMs),
                views, carts, checkouts, purchases,
                rate(carts, views),
                rate(checkouts, carts),
                rate(purchases, checkouts),
                rate(purchases, views));

        context().forward(new Record<>(OUTPUT_KEY, snapshot, wallClockMs));
    }

    private long count(FunnelStage stage) {
        Long value = counters.get(String.valueOf(stage.ordinal()));
        return value == null ? 0L : value;
    }

    private static double rate(long numerator, long denominator) {
        return denominator == 0 ? 0.0 : (double) numerator / denominator;
    }
}

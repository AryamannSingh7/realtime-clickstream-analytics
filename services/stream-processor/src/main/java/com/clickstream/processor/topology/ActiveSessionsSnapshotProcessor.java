package com.clickstream.processor.topology;

import com.clickstream.avro.ActiveSessionsSnapshot;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

/**
 * M6.4 stage 2 — global active-session aggregator.
 *
 * <p>Receives each partition's latest active-visitor count (converged onto one task by a
 * single-partition repartition), keeps the most recent value per partition in a state store, and
 * on a wall-clock cadence sums them into a global {@link ActiveSessionsSnapshot} emitted to
 * {@link StreamTopics#ACTIVE_SESSIONS}. Because visitors never span partitions, the sum is the
 * exact global distinct-active-visitor total.
 */
public class ActiveSessionsSnapshotProcessor extends ContextualProcessor<String, Long, String, ActiveSessionsSnapshot> {

    static final String STORE_NAME = "active-sessions-counter-store";
    static final String OUTPUT_KEY = "active";
    private static final Duration SNAPSHOT_INTERVAL = Duration.ofSeconds(3);
    private static final long WINDOW_SECONDS = StreamTopics.SESSION_INACTIVITY_GAP.toSeconds();

    private KeyValueStore<String, Long> partials;

    @Override
    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, ActiveSessionsSnapshot> context) {
        super.init(context);
        this.partials = context.getStateStore(STORE_NAME);
        context.schedule(SNAPSHOT_INTERVAL, PunctuationType.WALL_CLOCK_TIME, this::emitSnapshot);
    }

    @Override
    public void process(Record<String, Long> record) {
        partials.put(record.key(), record.value());
    }

    private void emitSnapshot(long wallClockMs) {
        long total = 0L;
        try (KeyValueIterator<String, Long> it = partials.all()) {
            while (it.hasNext()) {
                total += it.next().value;
            }
        }
        ActiveSessionsSnapshot snapshot = new ActiveSessionsSnapshot(
                Instant.ofEpochMilli(wallClockMs), total, WINDOW_SECONDS);
        context().forward(new Record<>(OUTPUT_KEY, snapshot, wallClockMs));
    }
}

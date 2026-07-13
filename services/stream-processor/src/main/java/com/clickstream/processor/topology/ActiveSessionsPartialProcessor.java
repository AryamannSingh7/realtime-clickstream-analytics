package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * M6.4 stage 1 — per-partition active-visitor tracker.
 *
 * <p>Runs co-partitioned with the source (events are keyed by {@code anonymous_id}, so no
 * repartition is needed), recording each visitor's most recent event time in a state store. On a
 * wall-clock cadence it counts the visitors whose last event falls within
 * {@link StreamTopics#SESSION_INACTIVITY_GAP} of the latest event time this partition has seen —
 * its event-time notion of "now" — purges the visitors that have gone quiet (bounding the store),
 * and forwards that partial count keyed by partition.
 *
 * <p>A visitor lives on exactly one partition, so a single downstream task can sum the per-partition
 * partials into an exact global distinct-visitor total without double counting.
 */
public class ActiveSessionsPartialProcessor extends ContextualProcessor<String, ClickEvent, String, Long> {

    static final String STORE_NAME = "active-sessions-visitor-store";

    /** How often each partition recomputes and emits its active-visitor count. */
    private static final Duration EMIT_INTERVAL = Duration.ofSeconds(3);

    /** A visitor is "active" while its last event is newer than latest_event_time - this window. */
    private static final long ACTIVE_WINDOW_MS = StreamTopics.SESSION_INACTIVITY_GAP.toMillis();

    private KeyValueStore<String, Long> lastSeen;
    private String partitionKey;
    private long maxEventTime;

    @Override
    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, Long> context) {
        super.init(context);
        this.lastSeen = context.getStateStore(STORE_NAME);
        this.partitionKey = String.valueOf(context.taskId().partition());
        // Restore the event-time "now" from the store so a restart doesn't briefly under-count.
        this.maxEventTime = seedMaxEventTime();
        context.schedule(EMIT_INTERVAL, PunctuationType.WALL_CLOCK_TIME, this::emitPartial);
    }

    @Override
    public void process(Record<String, ClickEvent> record) {
        long eventTime = record.timestamp();
        String visitor = record.key();
        Long previous = lastSeen.get(visitor);
        if (previous == null || eventTime > previous) {
            lastSeen.put(visitor, eventTime);
        }
        if (eventTime > maxEventTime) {
            maxEventTime = eventTime;
        }
    }

    private void emitPartial(long wallClockMs) {
        if (maxEventTime == 0L) {
            return; // no events observed on this partition yet
        }
        long cutoff = maxEventTime - ACTIVE_WINDOW_MS;
        long active = 0L;
        List<String> expired = new ArrayList<>();
        try (KeyValueIterator<String, Long> it = lastSeen.all()) {
            while (it.hasNext()) {
                var entry = it.next();
                if (entry.value > cutoff) {
                    active++;
                } else {
                    expired.add(entry.key);
                }
            }
        }
        for (String visitor : expired) {
            lastSeen.delete(visitor);
        }
        context().forward(new Record<>(partitionKey, active, wallClockMs));
    }

    private long seedMaxEventTime() {
        long max = 0L;
        try (KeyValueIterator<String, Long> it = lastSeen.all()) {
            while (it.hasNext()) {
                long value = it.next().value;
                if (value > max) {
                    max = value;
                }
            }
        }
        return max;
    }
}

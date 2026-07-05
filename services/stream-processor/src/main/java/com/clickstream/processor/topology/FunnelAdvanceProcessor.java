package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * M3.3 stage 1 — per-visitor funnel progress.
 *
 * <p>Runs co-partitioned with the source (events are keyed by {@code anonymous_id}, so no
 * repartition is needed), keeping each visitor's furthest funnel stage in a state store.
 * When an event advances a visitor to a new furthest stage, it forwards one delta record per
 * <em>newly reached</em> stage — all under the single global key {@link #GLOBAL_KEY} — so the
 * downstream aggregator can tally distinct-visitor counts without ever double counting.
 *
 * <p>Reaching a later stage implies every earlier one (a visitor whose first funnel event is a
 * purchase still contributes to view/cart/checkout), which keeps the aggregate funnel monotonic.
 */
public class FunnelAdvanceProcessor extends ContextualProcessor<String, ClickEvent, String, Integer> {

    static final String STORE_NAME = "funnel-visitor-store";

    /** Every delta is keyed the same so the whole funnel converges on one downstream task. */
    static final String GLOBAL_KEY = "funnel";

    private KeyValueStore<String, Integer> furthestStage;

    @Override
    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, Integer> context) {
        super.init(context);
        this.furthestStage = context.getStateStore(STORE_NAME);
    }

    @Override
    public void process(Record<String, ClickEvent> record) {
        int stage = FunnelStage.indexOf(record.value().getEventType());
        if (stage < 0) {
            return; // not a funnel step
        }

        String visitor = record.key();
        Integer previous = furthestStage.get(visitor);
        int from = previous == null ? -1 : previous;
        if (stage <= from) {
            return; // no forward progress for this visitor
        }

        // Credit every stage this visitor has now reached for the first time.
        for (int reached = from + 1; reached <= stage; reached++) {
            context().forward(new Record<>(GLOBAL_KEY, reached, record.timestamp()));
        }
        furthestStage.put(visitor, stage);
    }
}

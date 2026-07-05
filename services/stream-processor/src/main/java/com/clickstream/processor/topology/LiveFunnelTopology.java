package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.FunnelSnapshot;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * M3.3 — live cumulative conversion funnel.
 *
 * <p>Two-stage Processor-API pipeline. {@link FunnelAdvanceProcessor} tracks each visitor's
 * furthest funnel stage in a partitioned store (co-located by {@code anonymous_id}, so it
 * scales) and emits small stage-advance deltas under a single key. A repartition converges
 * those deltas onto one task, where {@link FunnelSnapshotProcessor} tallies global
 * distinct-visitor counts and emits a {@link FunnelSnapshot} on a wall-clock cadence to
 * {@link StreamTopics#FUNNEL_LIVE}.
 */
public final class LiveFunnelTopology {

    private LiveFunnelTopology() {
    }

    public static void build(
            StreamsBuilder builder,
            KStream<String, ClickEvent> source,
            Serde<ClickEvent> clickEventSerde,
            Serde<FunnelSnapshot> funnelSnapshotSerde) {

        StoreBuilder<org.apache.kafka.streams.state.KeyValueStore<String, Integer>> visitorStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(FunnelAdvanceProcessor.STORE_NAME),
                        Serdes.String(), Serdes.Integer());
        builder.addStateStore(visitorStore);

        StoreBuilder<org.apache.kafka.streams.state.KeyValueStore<String, Long>> counterStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(FunnelSnapshotProcessor.STORE_NAME),
                        Serdes.String(), Serdes.Long());
        builder.addStateStore(counterStore);

        source
                .process(FunnelAdvanceProcessor::new,
                        Named.as("funnel-advance-processor"),
                        FunnelAdvanceProcessor.STORE_NAME)
                // Collapse every visitor's deltas onto one task for a single global funnel.
                // One partition: the aggregation is global, so a fan-in to a single task is the
                // intent — otherwise idle repartition partitions spawn snapshot tasks with empty
                // stores whose punctuators emit spurious all-zero snapshots.
                .repartition(Repartitioned.with(Serdes.String(), Serdes.Integer())
                        .withName("funnel-delta-repartition")
                        .withNumberOfPartitions(1))
                .process(FunnelSnapshotProcessor::new,
                        Named.as("funnel-snapshot-processor"),
                        FunnelSnapshotProcessor.STORE_NAME)
                .to(StreamTopics.FUNNEL_LIVE, Produced.with(Serdes.String(), funnelSnapshotSerde));
    }
}

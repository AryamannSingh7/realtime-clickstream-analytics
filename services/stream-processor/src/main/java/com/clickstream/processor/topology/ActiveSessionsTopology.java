package com.clickstream.processor.topology;

import com.clickstream.avro.ActiveSessionsSnapshot;
import com.clickstream.avro.ClickEvent;
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
 * M6.4 — live active-session gauge.
 *
 * <p>Two-stage Processor-API pipeline mirroring the live funnel, chosen so the source events are
 * never repartitioned wholesale. {@link ActiveSessionsPartialProcessor} tracks each visitor's
 * last-seen time in a store co-located by {@code anonymous_id} and periodically forwards only its
 * partition's active-visitor count. A single-partition repartition converges those small partials
 * onto one task, where {@link ActiveSessionsSnapshotProcessor} sums them into a global
 * {@link ActiveSessionsSnapshot} on a wall-clock cadence to {@link StreamTopics#ACTIVE_SESSIONS}.
 */
public final class ActiveSessionsTopology {

    private ActiveSessionsTopology() {
    }

    public static void build(
            StreamsBuilder builder,
            KStream<String, ClickEvent> source,
            Serde<ActiveSessionsSnapshot> activeSessionsSnapshotSerde) {

        StoreBuilder<org.apache.kafka.streams.state.KeyValueStore<String, Long>> visitorStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(ActiveSessionsPartialProcessor.STORE_NAME),
                        Serdes.String(), Serdes.Long());
        builder.addStateStore(visitorStore);

        StoreBuilder<org.apache.kafka.streams.state.KeyValueStore<String, Long>> counterStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(ActiveSessionsSnapshotProcessor.STORE_NAME),
                        Serdes.String(), Serdes.Long());
        builder.addStateStore(counterStore);

        source
                .process(ActiveSessionsPartialProcessor::new,
                        Named.as("active-sessions-partial-processor"),
                        ActiveSessionsPartialProcessor.STORE_NAME)
                // One partition: the count is global, so the partials fan in to a single task.
                // Extra partitions would spawn snapshot tasks with empty stores emitting spurious
                // low counts — the same single-partition lesson as the funnel and anomaly rollups.
                .repartition(Repartitioned.with(Serdes.String(), Serdes.Long())
                        .withName("active-sessions-partial-repartition")
                        .withNumberOfPartitions(1))
                .process(ActiveSessionsSnapshotProcessor::new,
                        Named.as("active-sessions-snapshot-processor"),
                        ActiveSessionsSnapshotProcessor.STORE_NAME)
                .to(StreamTopics.ACTIVE_SESSIONS,
                        Produced.with(Serdes.String(), activeSessionsSnapshotSerde));
    }
}

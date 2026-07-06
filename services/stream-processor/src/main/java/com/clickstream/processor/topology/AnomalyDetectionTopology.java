package com.clickstream.processor.topology;

import com.clickstream.avro.AnomalyAlert;
import com.clickstream.avro.ClickEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * M4 — whole-stream volume anomaly detection.
 *
 * <p>Every event is mapped to a unit count under a single key and repartitioned onto one task,
 * so {@link EwmaAnomalyProcessor} observes the entire stream's per-bucket volume and maintains a
 * single EWMA baseline. Anomalies are written to {@link StreamTopics#ALERTS}.
 */
public final class AnomalyDetectionTopology {

    private AnomalyDetectionTopology() {
    }

    public static void build(
            StreamsBuilder builder,
            KStream<String, ClickEvent> source,
            Serde<AnomalyAlert> anomalyAlertSerde) {

        StoreBuilder<KeyValueStore<String, Double>> ewmaStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(EwmaAnomalyProcessor.STORE_NAME),
                        Serdes.String(), Serdes.Double());
        builder.addStateStore(ewmaStore);

        source
                .map((key, event) -> KeyValue.pair(EwmaAnomalyProcessor.OUTPUT_KEY, 1L),
                        Named.as("anomaly-unit-count"))
                // Global aggregation: collapse the whole stream onto one task. A single partition
                // is the intent (like the live funnel) — extra partitions would spawn detector
                // tasks with empty baselines that punctuate spurious alerts.
                .repartition(Repartitioned.with(Serdes.String(), Serdes.Long())
                        .withName("anomaly-count-repartition")
                        .withNumberOfPartitions(1))
                .process(EwmaAnomalyProcessor::new,
                        Named.as("anomaly-detector"),
                        EwmaAnomalyProcessor.STORE_NAME)
                .to(StreamTopics.ALERTS, Produced.with(Serdes.String(), anomalyAlertSerde));
    }
}

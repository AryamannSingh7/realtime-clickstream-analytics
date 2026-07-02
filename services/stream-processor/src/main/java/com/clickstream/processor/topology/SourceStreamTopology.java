package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Builds the source of the topology: the raw clickstream ({@code clickstream.events.raw}),
 * keyed by {@code anonymous_id}, deserialized from Avro via Schema Registry and stamped with
 * event-time. Every rollup ({@link MinuteMetricsTopology}, {@link TopNPagesTopology}, later
 * sessions/funnel) branches off the {@link KStream} produced here.
 *
 * <p>Kept as a plain builder (no Spring) so the running app and the {@code TopologyTestDriver}
 * tests construct the identical source.
 */
public final class SourceStreamTopology {

    private SourceStreamTopology() {
    }

    public static KStream<String, ClickEvent> source(
            StreamsBuilder builder, Serde<ClickEvent> clickEventSerde) {
        return builder.stream(
                StreamTopics.RAW_EVENTS,
                Consumed.with(Serdes.String(), clickEventSerde)
                        .withTimestampExtractor(new EventTimeExtractor())
                        .withName("raw-events-source"));
    }
}

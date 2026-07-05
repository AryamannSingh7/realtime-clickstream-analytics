package com.clickstream.processor.config;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.FunnelSnapshot;
import com.clickstream.processor.topology.LiveFunnelTopology;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

/**
 * Attaches the {@link LiveFunnelTopology} (and its two state stores) to the shared source
 * stream during context refresh, before the Streams factory bean builds and starts the topology.
 */
@Configuration
public class LiveFunnelConfig {

    @Autowired
    void buildPipeline(
            StreamsBuilder builder,
            KStream<String, ClickEvent> clickEventStream,
            SpecificAvroSerde<ClickEvent> clickEventSerde,
            SpecificAvroSerde<FunnelSnapshot> funnelSnapshotSerde) {
        LiveFunnelTopology.build(builder, clickEventStream, clickEventSerde, funnelSnapshotSerde);
    }
}

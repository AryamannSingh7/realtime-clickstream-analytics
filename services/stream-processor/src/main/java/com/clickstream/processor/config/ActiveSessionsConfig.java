package com.clickstream.processor.config;

import com.clickstream.avro.ActiveSessionsSnapshot;
import com.clickstream.avro.ClickEvent;
import com.clickstream.processor.topology.ActiveSessionsTopology;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

/**
 * Attaches the {@link ActiveSessionsTopology} (and its two state stores) to the shared source
 * stream during context refresh, before the Streams factory bean builds and starts the topology.
 */
@Configuration
public class ActiveSessionsConfig {

    @Autowired
    void buildPipeline(
            StreamsBuilder builder,
            KStream<String, ClickEvent> clickEventStream,
            SpecificAvroSerde<ActiveSessionsSnapshot> activeSessionsSnapshotSerde) {
        ActiveSessionsTopology.build(builder, clickEventStream, activeSessionsSnapshotSerde);
    }
}

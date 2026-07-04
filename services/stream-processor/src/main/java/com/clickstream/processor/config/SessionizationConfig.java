package com.clickstream.processor.config;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.SessionSummary;
import com.clickstream.processor.topology.SessionizationTopology;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

/**
 * Attaches the {@link SessionizationTopology} to the shared source stream during context
 * refresh (before the Streams factory bean builds and starts the topology).
 */
@Configuration
public class SessionizationConfig {

    @Autowired
    void buildPipeline(
            KStream<String, ClickEvent> clickEventStream,
            SpecificAvroSerde<ClickEvent> clickEventSerde,
            SpecificAvroSerde<SessionSummary> sessionSummarySerde) {
        SessionizationTopology.build(clickEventStream, clickEventSerde, sessionSummarySerde);
    }
}

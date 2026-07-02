package com.clickstream.processor.config;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.PageCount;
import com.clickstream.avro.PageTopN;
import com.clickstream.processor.topology.TopNPagesTopology;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

/**
 * Attaches the {@link TopNPagesTopology} (and its state store) to the shared source stream
 * during context refresh, before the Streams factory bean builds and starts the topology.
 */
@Configuration
public class TopNPagesConfig {

    @Autowired
    void buildPipeline(
            StreamsBuilder builder,
            KStream<String, ClickEvent> clickEventStream,
            SpecificAvroSerde<ClickEvent> clickEventSerde,
            SpecificAvroSerde<PageCount> pageCountSerde,
            SpecificAvroSerde<PageTopN> pageTopNSerde) {
        TopNPagesTopology.build(
                builder, clickEventStream, clickEventSerde, pageCountSerde, pageTopNSerde);
    }
}

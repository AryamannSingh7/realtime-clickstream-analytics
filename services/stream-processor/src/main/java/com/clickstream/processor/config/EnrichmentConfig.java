package com.clickstream.processor.config;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.EnrichedClickEvent;
import com.clickstream.avro.Product;
import com.clickstream.processor.topology.EnrichmentTopology;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

/**
 * Attaches the {@link EnrichmentTopology} to the shared source stream during context refresh.
 * Needs the {@link StreamsBuilder} directly to register the {@code ref.products} GlobalKTable.
 */
@Configuration
public class EnrichmentConfig {

    @Autowired
    void buildPipeline(
            StreamsBuilder builder,
            KStream<String, ClickEvent> clickEventStream,
            SpecificAvroSerde<Product> productSerde,
            SpecificAvroSerde<EnrichedClickEvent> enrichedClickEventSerde) {
        EnrichmentTopology.build(builder, clickEventStream, productSerde, enrichedClickEventSerde);
    }
}

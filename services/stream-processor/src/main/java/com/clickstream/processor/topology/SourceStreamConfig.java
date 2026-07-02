package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Publishes the event-time-stamped source {@link KStream} as a Spring bean and meters its
 * throughput. The stream itself is built by {@link SourceStreamTopology}; the rollup
 * configs consume this bean to attach their sub-topologies.
 */
@Configuration
public class SourceStreamConfig {

    @Bean
    public KStream<String, ClickEvent> clickEventStream(
            StreamsBuilder builder,
            SpecificAvroSerde<ClickEvent> clickEventSerde,
            MeterRegistry meterRegistry) {

        Counter consumed = Counter.builder("stream.events.consumed")
                .description("ClickEvents consumed from the raw topic by the stream processor")
                .register(meterRegistry);

        KStream<String, ClickEvent> events = SourceStreamTopology.source(builder, clickEventSerde);

        // Lightweight source-throughput visibility; the rollups branch off this same stream.
        events.peek((key, event) -> consumed.increment(), Named.as("meter-consumed"));

        return events;
    }
}

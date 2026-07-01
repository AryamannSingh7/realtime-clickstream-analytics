package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * The source of the topology: the raw clickstream ({@code clickstream.events.raw}),
 * keyed by {@code anonymous_id}, deserialized from Avro via Schema Registry and stamped
 * with event-time. Downstream milestones (per-minute metrics, top-N pages, sessions,
 * funnel, anomalies) all branch off the {@link KStream} published here.
 */
@Configuration
public class SourceStreamConfig {

    /** Raw ingress topic — Avro values, keyed by anonymous_id for per-user ordering. */
    public static final String RAW_EVENTS_TOPIC = "clickstream.events.raw";

    /**
     * Value serde for {@link ClickEvent}, backed by Schema Registry. Declared as a bean so
     * it is configured once and reused by every operator that (de)serializes click events.
     */
    @Bean
    public SpecificAvroSerde<ClickEvent> clickEventSerde(
            @Value("${spring.kafka.streams.properties.schema.registry.url}") String schemaRegistryUrl) {
        SpecificAvroSerde<ClickEvent> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(
                "schema.registry.url", schemaRegistryUrl,
                "specific.avro.reader", "true"
        ), false); // false -> configure as a value serde
        return serde;
    }

    /**
     * Publishes the event-time-stamped source stream. For now it only meters throughput;
     * subsequent milestones consume this {@code KStream} to build their rollups.
     */
    @Bean
    public KStream<String, ClickEvent> clickEventStream(
            StreamsBuilder builder,
            SpecificAvroSerde<ClickEvent> clickEventSerde,
            MeterRegistry meterRegistry) {

        Counter consumed = Counter.builder("stream.events.consumed")
                .description("ClickEvents consumed from the raw topic by the stream processor")
                .register(meterRegistry);

        KStream<String, ClickEvent> events = builder.stream(
                RAW_EVENTS_TOPIC,
                Consumed.with(Serdes.String(), clickEventSerde)
                        .withTimestampExtractor(new EventTimeExtractor())
                        .withName("raw-events-source"));

        // Lightweight source-throughput visibility; real processing is added next.
        events.peek((key, event) -> consumed.increment(), Named.as("meter-consumed"));

        return events;
    }
}

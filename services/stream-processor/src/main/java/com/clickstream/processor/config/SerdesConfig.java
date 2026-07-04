package com.clickstream.processor.config;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.MinuteMetrics;
import com.clickstream.avro.PageCount;
import com.clickstream.avro.PageTopN;
import com.clickstream.avro.SessionSummary;
import com.clickstream.processor.serde.AvroSerdes;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declares the Schema-Registry-backed Avro value serdes once, so every operator that
 * (de)serializes a given record type shares a single configured instance.
 */
@Configuration
public class SerdesConfig {

    private final String schemaRegistryUrl;

    public SerdesConfig(
            @Value("${spring.kafka.streams.properties.schema.registry.url}") String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Bean
    public SpecificAvroSerde<ClickEvent> clickEventSerde() {
        return AvroSerdes.forValue(schemaRegistryUrl);
    }

    @Bean
    public SpecificAvroSerde<MinuteMetrics> minuteMetricsSerde() {
        return AvroSerdes.forValue(schemaRegistryUrl);
    }

    @Bean
    public SpecificAvroSerde<PageCount> pageCountSerde() {
        return AvroSerdes.forValue(schemaRegistryUrl);
    }

    @Bean
    public SpecificAvroSerde<PageTopN> pageTopNSerde() {
        return AvroSerdes.forValue(schemaRegistryUrl);
    }

    @Bean
    public SpecificAvroSerde<SessionSummary> sessionSummarySerde() {
        return AvroSerdes.forValue(schemaRegistryUrl);
    }
}

package com.clickstream.processor.serde;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

import java.util.Map;

/**
 * Builds {@link SpecificAvroSerde}s configured against Schema Registry. Kept as a plain
 * factory (no Spring) so the exact same serdes back both the running topology and the
 * {@code TopologyTestDriver} tests (which point it at a {@code mock://} registry).
 */
public final class AvroSerdes {

    private AvroSerdes() {
    }

    /**
     * A value serde for the given specific-record type, reading records back as their
     * generated Java class rather than {@code GenericRecord}.
     */
    public static <T extends SpecificRecord> SpecificAvroSerde<T> forValue(String schemaRegistryUrl) {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true"
        ), false); // false -> configure as a value serde
        return serde;
    }
}

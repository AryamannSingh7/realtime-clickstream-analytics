package com.clickstream.generator.service;

import com.clickstream.generator.model.ReferenceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Seeds the compacted {@code ref.products} dimension topic from the canonical
 * {@link ReferenceData} catalog once the app is ready. The stream processor consumes this
 * topic as a {@code GlobalKTable} to enrich events with their product category (M3.2b).
 *
 * <p>Publishing on every startup is safe: the topic is log-compacted and keyed by SKU, so
 * re-sending the same catalog just overwrites each key with an identical value.
 */
@Component
public class ReferenceDataPublisher {

    private static final Logger log = LoggerFactory.getLogger(ReferenceDataPublisher.class);

    private final ReferenceData referenceData;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public ReferenceDataPublisher(
            ReferenceData referenceData,
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${generator.ref-products-topic:ref.products}") String topic) {
        this.referenceData = referenceData;
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void seedProducts() {
        int count = 0;
        for (ReferenceData.Product p : referenceData.products()) {
            com.clickstream.avro.Product record = com.clickstream.avro.Product.newBuilder()
                    .setId(p.id())
                    .setCategory(p.category())
                    .setPrice(p.price())
                    .build();
            kafkaTemplate.send(topic, record.getId(), record);
            count++;
        }
        kafkaTemplate.flush();
        log.info("Seeded {} products into reference topic '{}'", count, topic);
    }
}

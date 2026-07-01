package com.clickstream.processor.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * Enables Spring's Kafka Streams support: with {@code @EnableKafkaStreams} present,
 * Spring Boot auto-configures a {@code StreamsBuilderFactoryBean} from the
 * {@code spring.kafka.streams.*} properties (application id, EOS-v2, Schema Registry,
 * serdes) and injects a shared {@code StreamsBuilder} into topology {@code @Bean}s.
 *
 * <p>The streams runtime is held back (auto-startup disabled) until a topology exists;
 * source/processing beans are added from M2.2 onward.
 */
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {
}

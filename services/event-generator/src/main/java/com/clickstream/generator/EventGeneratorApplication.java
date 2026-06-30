package com.clickstream.generator;

import com.clickstream.generator.config.GeneratorProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Event generator service: synthesizes a realistic e-commerce clickstream and
 * produces Avro {@code ClickEvent}s to Kafka at a configurable rate.
 */
@SpringBootApplication
@EnableConfigurationProperties(GeneratorProperties.class)
public class EventGeneratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventGeneratorApplication.class, args);
    }
}

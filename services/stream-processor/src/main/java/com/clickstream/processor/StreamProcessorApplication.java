package com.clickstream.processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot host for the Kafka Streams topology that computes windowed rollups
 * (per-minute metrics, top-N pages, funnel, ...) over the raw clickstream.
 *
 * <p>The topology itself is assembled from {@code @Bean}-contributed {@code KStream}
 * definitions (added milestone-by-milestone); this host wires Streams config, EOS,
 * lifecycle and actuator/metrics.
 */
@SpringBootApplication
public class StreamProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamProcessorApplication.class, args);
    }
}

package com.clickstream.api.stream;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Enables the scheduled SSE heartbeat in {@link SseBroadcaster}. Kafka listeners are
 * auto-enabled by Spring Boot's Kafka auto-configuration (spring-kafka on the classpath).
 */
@Configuration
@EnableScheduling
public class StreamConfig {
}

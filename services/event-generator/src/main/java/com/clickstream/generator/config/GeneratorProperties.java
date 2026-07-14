package com.clickstream.generator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tunables for the synthetic clickstream. Bound from the {@code generator.*}
 * section of {@code application.yml} and overridable via environment variables.
 *
 * @param eps                 target throughput in events per second
 * @param virtualUsers        size of the pool of concurrent simulated sessions
 * @param emitterThreads      number of parallel producer threads; the target rate and
 *                            the session pool are split evenly across them so a single
 *                            box can drive high-throughput benchmarks (bounded by the
 *                            pool size)
 * @param topic               destination Kafka topic for raw events
 * @param lateEventRatio      fraction of events [0,1] stamped with a past event_time
 *                            to exercise event-time windowing / lateness handling
 * @param lateEventMaxDelayMs maximum backdating applied to a "late" event, in millis
 */
@ConfigurationProperties(prefix = "generator")
public record GeneratorProperties(
        int eps,
        int virtualUsers,
        int emitterThreads,
        String topic,
        double lateEventRatio,
        long lateEventMaxDelayMs) {

    public GeneratorProperties {
        if (eps < 1) {
            eps = 1;
        }
        if (virtualUsers < 1) {
            virtualUsers = 1;
        }
        if (emitterThreads < 1) {
            emitterThreads = 1;
        }
        if (topic == null || topic.isBlank()) {
            topic = "clickstream.events.raw";
        }
        if (lateEventRatio < 0) {
            lateEventRatio = 0;
        }
        if (lateEventRatio > 1) {
            lateEventRatio = 1;
        }
        if (lateEventMaxDelayMs < 0) {
            lateEventMaxDelayMs = 0;
        }
    }
}

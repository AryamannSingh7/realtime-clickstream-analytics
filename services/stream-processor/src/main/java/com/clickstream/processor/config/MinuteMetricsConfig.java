package com.clickstream.processor.config;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.MinuteMetrics;
import com.clickstream.processor.topology.MinuteMetricsTopology;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

/**
 * Attaches the {@link MinuteMetricsTopology} to the shared source stream during context
 * refresh (before the Streams factory bean builds and starts the topology).
 */
@Configuration
public class MinuteMetricsConfig {

    @Autowired
    void buildPipeline(
            KStream<String, ClickEvent> clickEventStream,
            SpecificAvroSerde<ClickEvent> clickEventSerde,
            SpecificAvroSerde<MinuteMetrics> minuteMetricsSerde) {
        MinuteMetricsTopology.build(clickEventStream, clickEventSerde, minuteMetricsSerde);
    }
}

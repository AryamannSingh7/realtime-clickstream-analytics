package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.EventType;
import com.clickstream.avro.MinuteMetrics;
import com.clickstream.processor.serde.AvroSerdes;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** M2.5 — {@link TopologyTestDriver} coverage for the per-minute rollup ({@link MinuteMetricsTopology}). */
class MinuteMetricsTopologyTest {

    private static final String SR_SCOPE = "minute-metrics-test";
    private static final String SR_URL = "mock://" + SR_SCOPE;

    private TopologyTestDriver driver;
    private TestInputTopic<String, ClickEvent> input;
    private TestOutputTopic<String, MinuteMetrics> output;

    @BeforeEach
    void setUp() {
        SpecificAvroSerde<ClickEvent> clickEventSerde = AvroSerdes.forValue(SR_URL);
        SpecificAvroSerde<MinuteMetrics> minuteMetricsSerde = AvroSerdes.forValue(SR_URL);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ClickEvent> source = SourceStreamTopology.source(builder, clickEventSerde);
        MinuteMetricsTopology.build(source, clickEventSerde, minuteMetricsSerde);
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-minute-metrics");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        driver = new TopologyTestDriver(topology, props);
        input = driver.createInputTopic(
                StreamTopics.RAW_EVENTS, new StringSerializer(), clickEventSerde.serializer());
        output = driver.createOutputTopic(
                StreamTopics.METRICS_1M, new StringDeserializer(), minuteMetricsSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(SR_SCOPE);
    }

    @Test
    void aggregatesEventTypeCountsAndRevenuePerWindow() {
        Instant windowStart = Instant.parse("2026-07-02T10:00:00Z");

        pipe(EventType.page_view, "/", windowStart.plusSeconds(1), null);
        pipe(EventType.page_view, "/product/1", windowStart.plusSeconds(2), null);
        pipe(EventType.add_to_cart, "/product/1", windowStart.plusSeconds(3), null);
        pipe(EventType.checkout_start, "/checkout", windowStart.plusSeconds(4), null);
        pipe(EventType.purchase, "/checkout", windowStart.plusSeconds(5), 49.99);
        pipe(EventType.click, "/", windowStart.plusSeconds(6), null); // counted only in `events`

        // An event well past window-end + grace advances stream time and flushes the window.
        pipe(EventType.page_view, "/", windowStart.plusSeconds(130), null);

        assertThat(output.getQueueSize()).isEqualTo(1);
        TestRecord<String, MinuteMetrics> record = output.readRecord();

        assertThat(record.key()).isEqualTo(String.valueOf(windowStart.toEpochMilli()));
        MinuteMetrics m = record.value();
        assertThat(m.getWindowStart()).isEqualTo(windowStart);
        assertThat(m.getWindowEnd()).isEqualTo(windowStart.plusSeconds(60));
        assertThat(m.getEvents()).isEqualTo(6);
        assertThat(m.getPageViews()).isEqualTo(2);
        assertThat(m.getAddToCart()).isEqualTo(1);
        assertThat(m.getCheckoutStart()).isEqualTo(1);
        assertThat(m.getPurchases()).isEqualTo(1);
        assertThat(m.getRevenue()).isEqualTo(49.99);
    }

    private void pipe(EventType type, String path, Instant time, Double revenue) {
        ClickEvent event = ClickEvents.event(type, path, time, revenue);
        input.pipeInput(event.getAnonymousId(), event, time);
    }
}

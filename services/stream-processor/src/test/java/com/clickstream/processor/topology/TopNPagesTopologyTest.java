package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.EventType;
import com.clickstream.avro.PageCount;
import com.clickstream.avro.PageTopN;
import com.clickstream.processor.serde.AvroSerdes;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
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
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** M2.5 — {@link TopologyTestDriver} coverage for the Processor-API top-N ranker ({@link TopNPagesTopology}). */
class TopNPagesTopologyTest {

    private static final String SR_SCOPE = "topn-pages-test";
    private static final String SR_URL = "mock://" + SR_SCOPE;

    private TopologyTestDriver driver;
    private TestInputTopic<String, ClickEvent> input;
    private TestOutputTopic<String, PageTopN> output;

    @BeforeEach
    void setUp() {
        SpecificAvroSerde<ClickEvent> clickEventSerde = AvroSerdes.forValue(SR_URL);
        SpecificAvroSerde<PageCount> pageCountSerde = AvroSerdes.forValue(SR_URL);
        SpecificAvroSerde<PageTopN> pageTopNSerde = AvroSerdes.forValue(SR_URL);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ClickEvent> source = SourceStreamTopology.source(builder, clickEventSerde);
        TopNPagesTopology.build(builder, source, clickEventSerde, pageCountSerde, pageTopNSerde);
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-topn-pages");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        driver = new TopologyTestDriver(topology, props);
        input = driver.createInputTopic(
                StreamTopics.RAW_EVENTS, new StringSerializer(), clickEventSerde.serializer());
        output = driver.createOutputTopic(
                StreamTopics.TOPN_PAGES_1M, new StringDeserializer(), pageTopNSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(SR_SCOPE);
    }

    @Test
    void ranksPagesByViewsPerWindowOnClose() {
        Instant windowStart = Instant.parse("2026-07-02T10:00:00Z");

        pageViews("/a", 5, windowStart);
        pageViews("/b", 3, windowStart);
        pageViews("/c", 1, windowStart);
        // Non-page_view events must not affect the ranking.
        pipe(EventType.add_to_cart, "/a", windowStart.plusSeconds(10));

        // Advance stream time past window-end + grace to close the window and fire the punctuator.
        pipe(EventType.page_view, "/next", windowStart.plusSeconds(130));

        assertThat(output.getQueueSize()).isEqualTo(1);
        TestRecord<String, PageTopN> record = output.readRecord();

        assertThat(record.key()).isEqualTo(String.valueOf(windowStart.toEpochMilli()));
        PageTopN topN = record.value();
        assertThat(topN.getWindowStart()).isEqualTo(windowStart);
        assertThat(topN.getWindowEnd()).isEqualTo(windowStart.plusSeconds(60));

        List<PageCount> entries = topN.getEntries();
        assertThat(entries).hasSize(3);
        assertThat(entries.get(0).getPath()).isEqualTo("/a");
        assertThat(entries.get(0).getViews()).isEqualTo(5);
        assertThat(entries.get(1).getPath()).isEqualTo("/b");
        assertThat(entries.get(1).getViews()).isEqualTo(3);
        assertThat(entries.get(2).getPath()).isEqualTo("/c");
        assertThat(entries.get(2).getViews()).isEqualTo(1);
    }

    private void pageViews(String path, int count, Instant windowStart) {
        for (int i = 0; i < count; i++) {
            pipe(EventType.page_view, path, windowStart.plusSeconds(i));
        }
    }

    private void pipe(EventType type, String path, Instant time) {
        ClickEvent event = ClickEvents.event(type, path, time);
        input.pipeInput(event.getAnonymousId(), event, time);
    }
}

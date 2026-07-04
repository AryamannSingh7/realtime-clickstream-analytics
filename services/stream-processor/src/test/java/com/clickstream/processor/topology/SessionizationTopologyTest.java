package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.EventType;
import com.clickstream.avro.SessionSummary;
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

/** M3.1 — {@link TopologyTestDriver} coverage for sessionization ({@link SessionizationTopology}). */
class SessionizationTopologyTest {

    private static final String SR_SCOPE = "sessionize-test";
    private static final String SR_URL = "mock://" + SR_SCOPE;

    private TopologyTestDriver driver;
    private TestInputTopic<String, ClickEvent> input;
    private TestOutputTopic<String, SessionSummary> output;

    @BeforeEach
    void setUp() {
        SpecificAvroSerde<ClickEvent> clickEventSerde = AvroSerdes.forValue(SR_URL);
        SpecificAvroSerde<SessionSummary> sessionSerde = AvroSerdes.forValue(SR_URL);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ClickEvent> source = SourceStreamTopology.source(builder, clickEventSerde);
        SessionizationTopology.build(source, clickEventSerde, sessionSerde);
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-sessionize");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        driver = new TopologyTestDriver(topology, props);
        input = driver.createInputTopic(
                StreamTopics.RAW_EVENTS, new StringSerializer(), clickEventSerde.serializer());
        output = driver.createOutputTopic(
                StreamTopics.SESSIONS, new StringDeserializer(), sessionSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(SR_SCOPE);
    }

    @Test
    void aggregatesOneSessionWithCountsRevenueAndLandingPath() {
        Instant start = Instant.parse("2026-07-04T10:00:00Z");

        pipe("anon-1", null, EventType.page_view, "/", start.plusSeconds(0), null);
        pipe("anon-1", "user-7", EventType.login, "/login", start.plusSeconds(30), null);
        pipe("anon-1", "user-7", EventType.page_view, "/product/1", start.plusSeconds(60), null);
        pipe("anon-1", "user-7", EventType.add_to_cart, "/product/1", start.plusSeconds(90), null);
        pipe("anon-1", "user-7", EventType.checkout_start, "/checkout", start.plusSeconds(120), null);
        pipe("anon-1", "user-7", EventType.purchase, "/checkout", start.plusSeconds(150), 42.50);

        flushWith(start.plusSeconds(150));

        TestRecord<String, SessionSummary> record = single();
        SessionSummary s = record.value();
        assertThat(record.key()).isEqualTo("anon-1:" + start.toEpochMilli());
        assertThat(s.getSessionId()).isEqualTo("anon-1:" + start.toEpochMilli());
        assertThat(s.getAnonymousId()).isEqualTo("anon-1");
        assertThat(s.getUserId()).isEqualTo("user-7");
        assertThat(s.getSessionStart()).isEqualTo(start);
        assertThat(s.getSessionEnd()).isEqualTo(start.plusSeconds(150));
        assertThat(s.getDurationMs()).isEqualTo(150_000L);
        assertThat(s.getEvents()).isEqualTo(6);
        assertThat(s.getPageViews()).isEqualTo(2);
        assertThat(s.getAddToCart()).isEqualTo(1);
        assertThat(s.getCheckoutStart()).isEqualTo(1);
        assertThat(s.getPurchases()).isEqualTo(1);
        assertThat(s.getRevenue()).isEqualTo(42.50);
        assertThat(s.getEntryPath()).isEqualTo("/");
        assertThat(s.getConverted()).isTrue();
    }

    @Test
    void inactivityGapSplitsIntoTwoSessions() {
        Instant start = Instant.parse("2026-07-04T10:00:00Z");
        Instant afterGap = start.plusSeconds(31 * 60); // > 30-minute inactivity gap

        pipe("anon-1", null, EventType.page_view, "/", start, null);
        pipe("anon-1", null, EventType.page_view, "/product/1", start.plusSeconds(20), null);

        pipe("anon-1", null, EventType.page_view, "/deals", afterGap, null);

        // Advance stream time well past the second session's close to flush both.
        flushWith(afterGap.plusSeconds(35 * 60));

        List<TestRecord<String, SessionSummary>> records = output.readRecordsToList();
        assertThat(records).hasSize(2);

        SessionSummary first = records.get(0).value();
        assertThat(first.getEntryPath()).isEqualTo("/");
        assertThat(first.getEvents()).isEqualTo(2);
        assertThat(first.getConverted()).isFalse();

        SessionSummary second = records.get(1).value();
        assertThat(second.getSessionStart()).isEqualTo(afterGap);
        assertThat(second.getEntryPath()).isEqualTo("/deals");
        assertThat(second.getEvents()).isEqualTo(1);
    }

    private TestRecord<String, SessionSummary> single() {
        assertThat(output.getQueueSize()).isEqualTo(1);
        return output.readRecord();
    }

    /** Emits a far-future event for a different visitor to push stream time past window close. */
    private void flushWith(Instant closeAfter) {
        Instant advance = closeAfter
                .plus(StreamTopics.SESSION_INACTIVITY_GAP)
                .plus(StreamTopics.SESSION_GRACE)
                .plusSeconds(1);
        pipe("anon-flush", null, EventType.page_view, "/", advance, null);
    }

    private void pipe(String anonymousId, String userId, EventType type, String path, Instant time, Double revenue) {
        ClickEvent event = ClickEvents.event(anonymousId, userId, type, path, time, revenue);
        input.pipeInput(event.getAnonymousId(), event, time);
    }
}

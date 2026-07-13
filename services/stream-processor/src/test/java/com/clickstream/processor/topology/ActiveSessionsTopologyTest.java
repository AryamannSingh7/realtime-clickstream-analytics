package com.clickstream.processor.topology;

import com.clickstream.avro.ActiveSessionsSnapshot;
import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.EventType;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** M6.4 — {@link TopologyTestDriver} coverage for the live active-session gauge ({@link ActiveSessionsTopology}). */
class ActiveSessionsTopologyTest {

    private static final String SR_SCOPE = "active-sessions-test";
    private static final String SR_URL = "mock://" + SR_SCOPE;

    private TopologyTestDriver driver;
    private TestInputTopic<String, ClickEvent> input;
    private TestOutputTopic<String, ActiveSessionsSnapshot> output;

    @BeforeEach
    void setUp() {
        SpecificAvroSerde<ClickEvent> clickEventSerde = AvroSerdes.forValue(SR_URL);
        SpecificAvroSerde<ActiveSessionsSnapshot> snapshotSerde = AvroSerdes.forValue(SR_URL);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ClickEvent> source = SourceStreamTopology.source(builder, clickEventSerde);
        ActiveSessionsTopology.build(builder, source, snapshotSerde);
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-active-sessions");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        driver = new TopologyTestDriver(topology, props);
        input = driver.createInputTopic(
                StreamTopics.RAW_EVENTS, new StringSerializer(), clickEventSerde.serializer());
        output = driver.createOutputTopic(
                StreamTopics.ACTIVE_SESSIONS, new StringDeserializer(), snapshotSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(SR_SCOPE);
    }

    @Test
    void countsDistinctVisitorsActiveWithinTheInactivityGap() {
        Instant t = Instant.parse("2026-07-05T10:00:00Z");

        pipe("anon-A", t);
        pipe("anon-B", t.plusSeconds(30));
        pipe("anon-A", t.plusSeconds(45)); // A again — a repeat visitor must still count once
        pipe("anon-C", t.plusSeconds(60));

        ActiveSessionsSnapshot snapshot = latestSnapshot();

        assertThat(snapshot.getActiveSessions()).isEqualTo(3);
        assertThat(snapshot.getWindowSeconds())
                .isEqualTo(StreamTopics.SESSION_INACTIVITY_GAP.toSeconds());
    }

    @Test
    void dropsVisitorsIdleBeyondTheInactivityGap() {
        Instant t = Instant.parse("2026-07-05T10:00:00Z");

        // anon-recent's event advances the event-time "now" 31 minutes past anon-old, pushing
        // anon-old outside the 30-minute active window so only anon-recent remains active.
        pipe("anon-old", t);
        pipe("anon-recent", t.plus(Duration.ofMinutes(31)));

        ActiveSessionsSnapshot snapshot = latestSnapshot();

        assertThat(snapshot.getActiveSessions()).isEqualTo(1);
    }

    /**
     * Two wall-clock advances: the first fires stage 1's punctuator (flushing per-partition partials
     * through the repartition into stage 2's store); the second fires stage 2's punctuator, which
     * sums them. The latest snapshot is the settled global count.
     */
    private ActiveSessionsSnapshot latestSnapshot() {
        driver.advanceWallClockTime(Duration.ofSeconds(3));
        driver.advanceWallClockTime(Duration.ofSeconds(3));
        List<ActiveSessionsSnapshot> snapshots = output.readValuesToList();
        assertThat(snapshots).isNotEmpty();
        return snapshots.get(snapshots.size() - 1);
    }

    private void pipe(String anonymousId, Instant time) {
        ClickEvent event = ClickEvents.event(anonymousId, null, EventType.page_view, "/", time, null);
        input.pipeInput(anonymousId, event, time);
    }
}

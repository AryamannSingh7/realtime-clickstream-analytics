package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.EventType;
import com.clickstream.avro.FunnelSnapshot;
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
import static org.assertj.core.api.Assertions.within;

/** M3.3 — {@link TopologyTestDriver} coverage for the live conversion funnel ({@link LiveFunnelTopology}). */
class LiveFunnelTopologyTest {

    private static final String SR_SCOPE = "live-funnel-test";
    private static final String SR_URL = "mock://" + SR_SCOPE;

    private TopologyTestDriver driver;
    private TestInputTopic<String, ClickEvent> input;
    private TestOutputTopic<String, FunnelSnapshot> output;

    @BeforeEach
    void setUp() {
        SpecificAvroSerde<ClickEvent> clickEventSerde = AvroSerdes.forValue(SR_URL);
        SpecificAvroSerde<FunnelSnapshot> funnelSnapshotSerde = AvroSerdes.forValue(SR_URL);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ClickEvent> source = SourceStreamTopology.source(builder, clickEventSerde);
        LiveFunnelTopology.build(builder, source, clickEventSerde, funnelSnapshotSerde);
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-live-funnel");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        driver = new TopologyTestDriver(topology, props);
        input = driver.createInputTopic(
                StreamTopics.RAW_EVENTS, new StringSerializer(), clickEventSerde.serializer());
        output = driver.createOutputTopic(
                StreamTopics.FUNNEL_LIVE, new StringDeserializer(), funnelSnapshotSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(SR_SCOPE);
    }

    @Test
    void talliesDistinctVisitorsPerStageWithConversionRates() {
        Instant t = Instant.parse("2026-07-05T10:00:00Z");

        // Visitor A walks the whole funnel; the repeated page views must count once.
        pipe("anon-A", EventType.page_view, t);
        pipe("anon-A", EventType.page_view, t.plusSeconds(1));
        pipe("anon-A", EventType.add_to_cart, t.plusSeconds(2));
        pipe("anon-A", EventType.checkout_start, t.plusSeconds(3));
        pipe("anon-A", EventType.purchase, t.plusSeconds(4));
        // Visitor B stops at the cart.
        pipe("anon-B", EventType.page_view, t.plusSeconds(1));
        pipe("anon-B", EventType.add_to_cart, t.plusSeconds(2));
        // Visitor C only ever views.
        pipe("anon-C", EventType.page_view, t.plusSeconds(1));
        // A non-funnel event must not create a visitor or move any counter.
        pipe("anon-D", EventType.search, t.plusSeconds(1));

        FunnelSnapshot snapshot = latestSnapshotAfterPunctuation();

        assertThat(snapshot.getViews()).isEqualTo(3);
        assertThat(snapshot.getAddToCart()).isEqualTo(2);
        assertThat(snapshot.getCheckoutStart()).isEqualTo(1);
        assertThat(snapshot.getPurchases()).isEqualTo(1);

        assertThat(snapshot.getViewToCartRate()).isCloseTo(2.0 / 3.0, within(1e-9));
        assertThat(snapshot.getCartToCheckoutRate()).isCloseTo(1.0 / 2.0, within(1e-9));
        assertThat(snapshot.getCheckoutToPurchaseRate()).isCloseTo(1.0, within(1e-9));
        assertThat(snapshot.getOverallConversionRate()).isCloseTo(1.0 / 3.0, within(1e-9));
    }

    @Test
    void aBarePurchaseCreditsEveryEarlierStage() {
        Instant t = Instant.parse("2026-07-05T10:00:00Z");

        // A visitor whose only funnel event is a purchase must still count toward every stage,
        // keeping the funnel monotonic (views >= add_to_cart >= checkout_start >= purchases).
        pipe("anon-Z", EventType.purchase, t);

        FunnelSnapshot snapshot = latestSnapshotAfterPunctuation();

        assertThat(snapshot.getViews()).isEqualTo(1);
        assertThat(snapshot.getAddToCart()).isEqualTo(1);
        assertThat(snapshot.getCheckoutStart()).isEqualTo(1);
        assertThat(snapshot.getPurchases()).isEqualTo(1);
        assertThat(snapshot.getOverallConversionRate()).isCloseTo(1.0, within(1e-9));
    }

    /** Advances the wall clock to fire the snapshot punctuator and returns the latest snapshot. */
    private FunnelSnapshot latestSnapshotAfterPunctuation() {
        driver.advanceWallClockTime(Duration.ofSeconds(3));
        List<FunnelSnapshot> snapshots = output.readValuesToList();
        assertThat(snapshots).isNotEmpty();
        return snapshots.get(snapshots.size() - 1);
    }

    private void pipe(String anonymousId, EventType type, Instant time) {
        ClickEvent event = ClickEvents.event(anonymousId, null, type, "/", time, null);
        input.pipeInput(anonymousId, event, time);
    }
}

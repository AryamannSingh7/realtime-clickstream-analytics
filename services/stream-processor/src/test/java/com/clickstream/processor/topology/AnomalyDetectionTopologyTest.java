package com.clickstream.processor.topology;

import com.clickstream.avro.AnomalyAlert;
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

/** M4 — {@link TopologyTestDriver} coverage for the EWMA volume anomaly detector. */
class AnomalyDetectionTopologyTest {

    private static final String SR_SCOPE = "anomaly-test";
    private static final String SR_URL = "mock://" + SR_SCOPE;

    private TopologyTestDriver driver;
    private TestInputTopic<String, ClickEvent> input;
    private TestOutputTopic<String, AnomalyAlert> output;

    @BeforeEach
    void setUp() {
        SpecificAvroSerde<ClickEvent> clickEventSerde = AvroSerdes.forValue(SR_URL);
        SpecificAvroSerde<AnomalyAlert> anomalyAlertSerde = AvroSerdes.forValue(SR_URL);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ClickEvent> source = SourceStreamTopology.source(builder, clickEventSerde);
        AnomalyDetectionTopology.build(builder, source, anomalyAlertSerde);
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-anomaly");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        driver = new TopologyTestDriver(topology, props);
        input = driver.createInputTopic(
                StreamTopics.RAW_EVENTS, new StringSerializer(), clickEventSerde.serializer());
        output = driver.createOutputTopic(
                StreamTopics.ALERTS, new StringDeserializer(), anomalyAlertSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(SR_SCOPE);
    }

    @Test
    void firesSpikeAlertWhenVolumeJumpsAboveBaseline() {
        // Six flat buckets of 10 events establish a warmed baseline (mean 10).
        for (int i = 0; i < 6; i++) {
            closeBucketWith(10);
        }
        assertThat(output.readValuesToList()).isEmpty();

        // A bucket ten times the baseline must fire a spike.
        closeBucketWith(100);

        List<AnomalyAlert> alerts = output.readValuesToList();
        assertThat(alerts).hasSize(1);
        AnomalyAlert alert = alerts.get(0);
        assertThat(alert.getObserved()).isEqualTo(100);
        assertThat(alert.getDirection()).isEqualTo("spike");
        assertThat(alert.getZScore()).isGreaterThanOrEqualTo(EwmaAnomalyProcessor.THRESHOLD_K);
        assertThat(alert.getBaselineMean()).isEqualTo(10.0);
    }

    @Test
    void firesDropAlertWhenVolumeCollapses() {
        for (int i = 0; i < 6; i++) {
            closeBucketWith(100);
        }
        assertThat(output.readValuesToList()).isEmpty();

        // Traffic falling to zero is a drop.
        closeBucketWith(0);

        List<AnomalyAlert> alerts = output.readValuesToList();
        assertThat(alerts).hasSize(1);
        AnomalyAlert alert = alerts.get(0);
        assertThat(alert.getObserved()).isEqualTo(0);
        assertThat(alert.getDirection()).isEqualTo("drop");
        assertThat(alert.getZScore()).isLessThanOrEqualTo(-EwmaAnomalyProcessor.THRESHOLD_K);
    }

    @Test
    void staysQuietWhileWarmingUp() {
        // A wild jump inside the warmup window must not alert — the baseline isn't trusted yet.
        closeBucketWith(10);
        closeBucketWith(200);

        assertThat(output.readValuesToList()).isEmpty();
    }

    /** Emits {@code count} events, then advances the wall clock to close the bucket. */
    private void closeBucketWith(int count) {
        Instant t = Instant.parse("2026-07-06T10:00:00Z");
        for (int i = 0; i < count; i++) {
            ClickEvent event = ClickEvents.event(EventType.page_view, "/", t);
            input.pipeInput("anon-" + i, event, t);
        }
        driver.advanceWallClockTime(EwmaAnomalyProcessor.BUCKET_INTERVAL);
    }
}

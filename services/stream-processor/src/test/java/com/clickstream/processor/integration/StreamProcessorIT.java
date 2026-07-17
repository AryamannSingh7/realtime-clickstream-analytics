package com.clickstream.processor.integration;

import static com.clickstream.processor.topology.StreamTopics.FUNNEL_LIVE;
import static com.clickstream.processor.topology.StreamTopics.RAW_EVENTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.Device;
import com.clickstream.avro.EventType;
import com.clickstream.avro.FunnelSnapshot;
import com.clickstream.avro.Geo;
import com.clickstream.processor.serde.AvroSerdes;
import com.clickstream.processor.topology.LiveFunnelTopology;
import com.clickstream.processor.topology.SourceStreamTopology;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end integration test for the live-funnel topology against a <em>real</em> Kafka broker
 * and Schema Registry (Testcontainers). Unlike the {@code TopologyTestDriver} unit tests, this
 * exercises real Avro serialization over Schema Registry, real EOS-v2 processing across a broker,
 * and the wall-clock punctuator — the parts a broker-less driver can't cover.
 *
 * <p>The topology is assembled from the same production builders the running app wires
 * ({@link SourceStreamTopology#source} + {@link LiveFunnelTopology#build}) and driven by a plain
 * {@link KafkaStreams} instance, so the test stays focused on the streaming path without booting
 * the full Spring context.
 */
@Testcontainers
class StreamProcessorIT {

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.8.0");
    private static final DockerImageName SR_IMAGE = DockerImageName.parse("confluentinc/cp-schema-registry:7.8.0");

    /** Internal listener the Schema Registry container reaches the broker on (over the shared network). */
    private static final String KAFKA_INTERNAL = "kafka:19092";

    private static final Network NETWORK = Network.newNetwork();

    @Container
    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(KAFKA_IMAGE)
                    .withNetwork(NETWORK)
                    .withListener(KAFKA_INTERNAL);

    @Container
    static final GenericContainer<?> SCHEMA_REGISTRY =
            new GenericContainer<>(SR_IMAGE)
                    .withNetwork(NETWORK)
                    .withExposedPorts(8081)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + KAFKA_INTERNAL)
                    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
                    .dependsOn(KAFKA);

    private static String schemaRegistryUrl() {
        return "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    }

    @Test
    void producesLiveFunnelSnapshotFromRealJourneys() throws Exception {
        // Source + live-funnel output must exist before the topology starts.
        createTopics(RAW_EVENTS, FUNNEL_LIVE);

        // Seed four journeys. Three convert fully (view->cart->checkout->purchase); one bounces
        // after a single page_view. Expected cumulative funnel: views 4, cart/checkout/purchase 3.
        Instant base = Instant.now();
        try (KafkaProducer<String, ClickEvent> producer = avroProducer()) {
            for (String visitor : List.of("anon-a", "anon-b", "anon-c")) {
                send(producer, visitor, EventType.page_view, "/", base);
                send(producer, visitor, EventType.add_to_cart, "/cart", base.plusMillis(10));
                send(producer, visitor, EventType.checkout_start, "/checkout", base.plusMillis(20));
                send(producer, visitor, EventType.purchase, "/thank-you", base.plusMillis(30));
            }
            send(producer, "anon-d", EventType.page_view, "/", base);
            producer.flush();
        }

        KafkaStreams streams = buildStreams();
        AtomicReference<FunnelSnapshot> latest = new AtomicReference<>();
        try {
            streams.start();

            // The FunnelSnapshotProcessor emits on a 3s wall-clock cadence; await the converged one.
            try (KafkaConsumer<String, FunnelSnapshot> consumer = avroConsumer()) {
                consumer.subscribe(List.of(FUNNEL_LIVE));
                await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
                    consumer.poll(Duration.ofMillis(500)).forEach(rec -> latest.set(rec.value()));
                    FunnelSnapshot snapshot = latest.get();
                    assertThat(snapshot).as("a funnel snapshot should have been emitted").isNotNull();
                    assertThat(snapshot.getViews()).isEqualTo(4L);
                    assertThat(snapshot.getAddToCart()).isEqualTo(3L);
                    assertThat(snapshot.getCheckoutStart()).isEqualTo(3L);
                    assertThat(snapshot.getPurchases()).isEqualTo(3L);
                });
            }
        } finally {
            streams.close(Duration.ofSeconds(30));
        }

        FunnelSnapshot snapshot = latest.get();
        assertThat(snapshot.getViewToCartRate()).isEqualTo(3.0 / 4.0);
        assertThat(snapshot.getCartToCheckoutRate()).isEqualTo(1.0);
        assertThat(snapshot.getCheckoutToPurchaseRate()).isEqualTo(1.0);
        assertThat(snapshot.getOverallConversionRate()).isEqualTo(3.0 / 4.0);
    }

    /** Assemble source + live-funnel from the production builders and back them with real serdes. */
    private static KafkaStreams buildStreams() throws Exception {
        String srUrl = schemaRegistryUrl();
        SpecificAvroSerde<ClickEvent> clickEventSerde = AvroSerdes.forValue(srUrl);
        SpecificAvroSerde<FunnelSnapshot> funnelSnapshotSerde = AvroSerdes.forValue(srUrl);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ClickEvent> source = SourceStreamTopology.source(builder, clickEventSerde);
        LiveFunnelTopology.build(builder, source, clickEventSerde, funnelSnapshotSerde);
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "it-stream-processor-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("kafka-streams-it").toString());
        return new KafkaStreams(topology, props);
    }

    private static void createTopics(String... names) throws Exception {
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            List<NewTopic> topics = Arrays.stream(names)
                    .map(name -> new NewTopic(name, 1, (short) 1))
                    .toList();
            admin.createTopics(topics).all().get();
        }
    }

    private static KafkaProducer<String, ClickEvent> avroProducer() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl());
        return new KafkaProducer<>(config);
    }

    private static KafkaConsumer<String, FunnelSnapshot> avroConsumer() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "it-funnel-consumer-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl());
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return new KafkaConsumer<>(config);
    }

    private static void send(KafkaProducer<String, ClickEvent> producer, String anonymousId,
                             EventType type, String path, Instant eventTime) {
        ClickEvent event = ClickEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(type)
                .setAnonymousId(anonymousId)
                .setUserId(null)
                .setEventTime(eventTime)
                .setPath(path)
                .setDevice(new Device("desktop", "macos", "chrome"))
                .setGeo(new Geo("US", "New York"))
                .build();
        producer.send(new ProducerRecord<>(RAW_EVENTS, anonymousId, event));
    }
}

package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.EnrichedClickEvent;
import com.clickstream.avro.EventType;
import com.clickstream.avro.Product;
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

import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** M3.2b — {@link TopologyTestDriver} coverage for product enrichment ({@link EnrichmentTopology}). */
class EnrichmentTopologyTest {

    private static final String SR_SCOPE = "enrichment-test";
    private static final String SR_URL = "mock://" + SR_SCOPE;
    private static final Instant T = Instant.parse("2026-07-04T10:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, ClickEvent> events;
    private TestInputTopic<String, Product> products;
    private TestOutputTopic<String, EnrichedClickEvent> enriched;

    @BeforeEach
    void setUp() {
        SpecificAvroSerde<ClickEvent> clickEventSerde = AvroSerdes.forValue(SR_URL);
        SpecificAvroSerde<Product> productSerde = AvroSerdes.forValue(SR_URL);
        SpecificAvroSerde<EnrichedClickEvent> enrichedSerde = AvroSerdes.forValue(SR_URL);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ClickEvent> source = SourceStreamTopology.source(builder, clickEventSerde);
        EnrichmentTopology.build(builder, source, productSerde, enrichedSerde);
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-enrichment");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        driver = new TopologyTestDriver(topology, props);
        events = driver.createInputTopic(
                StreamTopics.RAW_EVENTS, new StringSerializer(), clickEventSerde.serializer());
        products = driver.createInputTopic(
                StreamTopics.REF_PRODUCTS, new StringSerializer(), productSerde.serializer());
        enriched = driver.createOutputTopic(
                StreamTopics.ENRICHED_EVENTS, new StringDeserializer(), enrichedSerde.deserializer());

        // Seed the product dimension before any event flows.
        products.pipeInput("sku-1001", new Product("sku-1001", "electronics", 199.99));
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(SR_SCOPE);
    }

    @Test
    void enrichesEventWithMatchingProductCategory() {
        ClickEvent event = ClickEvents.withProduct(EventType.add_to_cart, "/product/sku-1001", T, "sku-1001", 199.99);
        events.pipeInput(event.getAnonymousId(), event, T);

        EnrichedClickEvent out = enriched.readValue();
        assertThat(out.getProductId()).isEqualTo("sku-1001");
        assertThat(out.getProductCategory()).isEqualTo("electronics");
        assertThat(out.getProductRefPrice()).isEqualTo(199.99);
        // Carried-through fields survive the join.
        assertThat(out.getEventType()).isEqualTo("add_to_cart");
        assertThat(out.getPath()).isEqualTo("/product/sku-1001");
        assertThat(out.getDeviceType()).isEqualTo("desktop");
        assertThat(out.getGeoCountry()).isEqualTo("US");
    }

    @Test
    void passesThroughUnknownSkuWithNullCategory() {
        ClickEvent event = ClickEvents.withProduct(EventType.add_to_cart, "/product/sku-9999", T, "sku-9999", 5.0);
        events.pipeInput(event.getAnonymousId(), event, T);

        EnrichedClickEvent out = enriched.readValue();
        assertThat(out.getProductId()).isEqualTo("sku-9999");
        assertThat(out.getProductCategory()).isNull();
        assertThat(out.getProductRefPrice()).isNull();
    }

    @Test
    void passesThroughEventWithNoProductId() {
        ClickEvent event = ClickEvents.event(EventType.page_view, "/", T);
        events.pipeInput(event.getAnonymousId(), event, T);

        EnrichedClickEvent out = enriched.readValue();
        assertThat(out.getProductId()).isNull();
        assertThat(out.getProductCategory()).isNull();
        assertThat(out.getEventType()).isEqualTo("page_view");
    }
}

package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.MinuteMetrics;
import com.clickstream.avro.PageCount;
import com.clickstream.avro.PageTopN;
import com.clickstream.processor.serde.AvroSerdes;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * M2.5 — asserts the three sub-topologies compose into one valid {@link Topology} off a
 * single source stream, mirroring the Spring wiring (source + minute-metrics + top-N).
 * Catches store-name / processor-name clashes the per-topology tests can't.
 */
class TopologyAssemblyTest {

    private static final String SR_SCOPE = "assembly-test";
    private static final String SR_URL = "mock://" + SR_SCOPE;

    @AfterEach
    void tearDown() {
        MockSchemaRegistry.dropScope(SR_SCOPE);
    }

    @Test
    void allSubTopologiesComposeOffOneSource() {
        StreamsBuilder builder = new StreamsBuilder();
        var clickEventSerde = AvroSerdes.<ClickEvent>forValue(SR_URL);
        KStream<String, ClickEvent> source = SourceStreamTopology.source(builder, clickEventSerde);

        MinuteMetricsTopology.build(source, clickEventSerde, AvroSerdes.<MinuteMetrics>forValue(SR_URL));
        TopNPagesTopology.build(builder, source, clickEventSerde,
                AvroSerdes.<PageCount>forValue(SR_URL), AvroSerdes.<PageTopN>forValue(SR_URL));

        assertThatCode(builder::build).doesNotThrowAnyException();

        String description = builder.build().describe().toString();
        assertThat(description)
                .contains(StreamTopics.RAW_EVENTS)
                .contains(StreamTopics.METRICS_1M)
                .contains(StreamTopics.TOPN_PAGES_1M);
    }
}

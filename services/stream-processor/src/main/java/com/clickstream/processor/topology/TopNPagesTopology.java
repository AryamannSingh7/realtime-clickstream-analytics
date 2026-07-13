package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.EventType;
import com.clickstream.avro.PageCount;
import com.clickstream.avro.PageTopN;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * M2.4 — windowed top-N most-viewed pages.
 *
 * <p>DSL half: filter to {@code page_view}, count per path per tumbling event-time window.
 * The count changelog is then rekeyed by window start and repartitioned so a window's every
 * path lands on one task, where {@link TopNPagesProcessor} (Processor API) ranks and emits
 * the top-N once the window closes. Output goes to {@link StreamTopics#TOPN_PAGES_1M}.
 */
public final class TopNPagesTopology {

    private TopNPagesTopology() {
    }

    /** How many pages to keep per window. */
    public static final int TOP_N = 10;

    public static void build(
            StreamsBuilder builder,
            KStream<String, ClickEvent> source,
            Serde<ClickEvent> clickEventSerde,
            Serde<PageCount> pageCountSerde,
            Serde<PageTopN> pageTopNSerde) {

        StoreBuilder<org.apache.kafka.streams.state.KeyValueStore<String, Long>> topNStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(TopNPagesProcessor.STORE_NAME),
                        Serdes.String(), Serdes.Long());
        builder.addStateStore(topNStore);

        source
                .filter((key, event) -> event.getEventType() == EventType.page_view,
                        Named.as("topn-filter-page-views"))
                .groupBy((key, event) -> event.getPath(),
                        Grouped.with("topn-pageview-group", Serdes.String(), clickEventSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(StreamTopics.WINDOW_SIZE, StreamTopics.WINDOW_GRACE))
                .count(Materialized.as("topn-page-view-counts"))
                .toStream()
                // Rekey by window start so all paths of a window co-locate; carry (path, count).
                .map((windowedKey, count) -> KeyValue.pair(
                                String.valueOf(windowedKey.window().start()),
                                new PageCount(windowedKey.key(), count)),
                        Named.as("topn-rekey-by-window"))
                // Single partition: records are keyed by window start, so a window's every path
                // must land on ONE task for its STREAM_TIME punctuator to advance and flush on time.
                // Inheriting the source's 6 partitions stalls each task until a window hashes to it
                // (~every 6th minute) — the "global aggregation is inherently single-partition" lesson.
                .repartition(Repartitioned.with(Serdes.String(), pageCountSerde)
                        .withName("topn-window-repartition")
                        .withNumberOfPartitions(1))
                .process(
                        () -> new TopNPagesProcessor(
                                StreamTopics.WINDOW_SIZE.toMillis(),
                                StreamTopics.WINDOW_GRACE.toMillis(),
                                TOP_N),
                        Named.as("topn-pages-processor"),
                        TopNPagesProcessor.STORE_NAME)
                .to(StreamTopics.TOPN_PAGES_1M, Produced.with(Serdes.String(), pageTopNSerde));
    }
}

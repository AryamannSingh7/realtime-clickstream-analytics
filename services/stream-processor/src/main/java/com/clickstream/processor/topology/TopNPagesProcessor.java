package com.clickstream.processor.topology;

import com.clickstream.avro.PageCount;
import com.clickstream.avro.PageTopN;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * M2.4 — the top-N ranker, written against the Processor API.
 *
 * <p>Upstream the DSL counts page views per {@code (path, window)} and repartitions the
 * changelog so every path of a given window arrives on the same task, keyed by the window
 * start (epoch millis, as a string). This processor keeps the latest count for each
 * {@code windowStart|path} in a state store and, on a stream-time punctuation, flushes the
 * top-N ranking for every window that has closed (window end + grace &le; stream time),
 * emitting one {@link PageTopN} per window and deleting its entries.
 *
 * <p>Reading from the count <em>changelog</em> and overwriting per key means the value held
 * at flush time is the final count for the window — no double counting across updates.
 */
public class TopNPagesProcessor extends ContextualProcessor<String, PageCount, String, PageTopN> {

    static final String STORE_NAME = "topn-pages-store";
    private static final Duration PUNCTUATE_INTERVAL = Duration.ofSeconds(15);
    private static final char SEP = '|';

    private final long windowSizeMs;
    private final long graceMs;
    private final int topN;

    private KeyValueStore<String, Long> store;

    public TopNPagesProcessor(long windowSizeMs, long graceMs, int topN) {
        this.windowSizeMs = windowSizeMs;
        this.graceMs = graceMs;
        this.topN = topN;
    }

    @Override
    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, PageTopN> context) {
        super.init(context);
        this.store = context.getStateStore(STORE_NAME);
        context.schedule(PUNCTUATE_INTERVAL, PunctuationType.STREAM_TIME, this::flushClosedWindows);
    }

    @Override
    public void process(Record<String, PageCount> record) {
        // key = window start (epoch millis); value carries the path and its latest view count.
        PageCount pageCount = record.value();
        store.put(record.key() + SEP + pageCount.getPath(), pageCount.getViews());
    }

    /** Emits the top-N for each closed window, then clears that window's entries. */
    private void flushClosedWindows(long streamTimeMs) {
        Map<Long, List<PageCount>> byWindow = new HashMap<>();
        List<String> toDelete = new ArrayList<>();

        try (KeyValueIterator<String, Long> it = store.all()) {
            while (it.hasNext()) {
                KeyValue<String, Long> entry = it.next();
                int sep = entry.key.indexOf(SEP);
                long windowStart = Long.parseLong(entry.key.substring(0, sep));
                String path = entry.key.substring(sep + 1);

                long windowEnd = windowStart + windowSizeMs;
                if (windowEnd + graceMs <= streamTimeMs) {
                    byWindow.computeIfAbsent(windowStart, k -> new ArrayList<>())
                            .add(new PageCount(path, entry.value));
                    toDelete.add(entry.key);
                }
            }
        }

        byWindow.forEach(this::emitWindow);
        toDelete.forEach(store::delete);
    }

    private void emitWindow(long windowStart, List<PageCount> pages) {
        pages.sort(Comparator.comparingLong(PageCount::getViews).reversed()
                .thenComparing(PageCount::getPath));
        List<PageCount> ranked = new ArrayList<>(pages.subList(0, Math.min(topN, pages.size())));

        long windowEnd = windowStart + windowSizeMs;
        PageTopN out = new PageTopN(
                Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowEnd), ranked);
        context().forward(new Record<>(String.valueOf(windowStart), out, windowEnd));
    }
}

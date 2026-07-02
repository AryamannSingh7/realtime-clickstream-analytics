package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.EventType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** M2.5 — unit coverage for the event-time extractor and its fallbacks. */
class EventTimeExtractorTest {

    private final EventTimeExtractor extractor = new EventTimeExtractor();

    @Test
    void usesEventTimeWhenPresent() {
        Instant eventTime = Instant.parse("2026-07-02T10:00:00Z");
        ClickEvent event = ClickEvents.event(EventType.page_view, "/", eventTime);

        long ts = extractor.extract(record(event, 999L), -1L);

        assertThat(ts).isEqualTo(eventTime.toEpochMilli());
    }

    @Test
    void fallsBackToRecordTimestampWhenValueIsNotClickEvent() {
        long ts = extractor.extract(record("not-an-event", 12345L), -1L);

        assertThat(ts).isEqualTo(12345L);
    }

    @Test
    void fallsBackToRecordTimestampWhenEventTimeIsNegative() {
        ClickEvent event = ClickEvents.event(EventType.page_view, "/", Instant.ofEpochMilli(-100L));

        long ts = extractor.extract(record(event, 777L), -1L);

        assertThat(ts).isEqualTo(777L);
    }

    @Test
    void fallsBackToPartitionTimeWhenNoUsableTimestamp() {
        ClickEvent event = ClickEvents.event(EventType.page_view, "/", Instant.ofEpochMilli(-100L));

        long ts = extractor.extract(record(event, -1L), 42L);

        assertThat(ts).isEqualTo(42L);
    }

    private static ConsumerRecord<Object, Object> record(Object value, long timestamp) {
        return new ConsumerRecord<>(
                StreamTopics.RAW_EVENTS, 0, 0L, timestamp, TimestampType.CREATE_TIME,
                0, 0, null, value, new RecordHeaders(), Optional.empty());
    }
}

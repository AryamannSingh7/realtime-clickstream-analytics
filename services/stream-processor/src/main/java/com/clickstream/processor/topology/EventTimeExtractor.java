package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Drives windowing off the domain event-time ({@link ClickEvent#getEventTime()}, epoch
 * millis) instead of the broker/ingest time, so windows and sessions reflect when the
 * interaction actually happened.
 *
 * <p>Falls back to the record (ingest) timestamp when event-time is missing or negative,
 * so a single malformed record cannot stall stream-time for the whole partition.
 */
public class EventTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof ClickEvent event && event.getEventTime() != null) {
            long eventTime = event.getEventTime().toEpochMilli();
            if (eventTime >= 0) {
                return eventTime;
            }
        }
        long recordTime = record.timestamp();
        return recordTime >= 0 ? recordTime : partitionTime;
    }
}

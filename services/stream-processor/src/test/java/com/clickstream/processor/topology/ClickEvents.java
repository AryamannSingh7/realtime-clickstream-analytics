package com.clickstream.processor.topology;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.Device;
import com.clickstream.avro.EventType;
import com.clickstream.avro.Geo;

import java.time.Instant;
import java.util.UUID;

/** Test factory for {@link ClickEvent}s with all required fields filled in. */
final class ClickEvents {

    private ClickEvents() {
    }

    static ClickEvent event(EventType type, String path, Instant eventTime) {
        return event(type, path, eventTime, null);
    }

    static ClickEvent event(EventType type, String path, Instant eventTime, Double revenue) {
        return ClickEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(type)
                .setAnonymousId("anon-1")
                .setEventTime(eventTime)
                .setPath(path)
                .setDevice(new Device("desktop", "macos", "chrome"))
                .setGeo(new Geo("US", "New York"))
                .setRevenue(revenue)
                .build();
    }
}

package com.clickstream.api.stream;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Server-Sent Events endpoints for the live {@code analytics.*} roll-ups. A browser
 * {@code EventSource("/api/stream/metrics")} receives every new record on that stream as a
 * named JSON SSE event; the connection auto-reconnects on drop.
 */
@RestController
@RequestMapping(path = "/api/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
public class StreamController {

    private final SseBroadcaster broadcaster;

    public StreamController(SseBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @GetMapping("/metrics")
    public SseEmitter metrics() {
        return broadcaster.subscribe(AnalyticsStreams.METRICS);
    }

    @GetMapping("/topn")
    public SseEmitter topN() {
        return broadcaster.subscribe(AnalyticsStreams.TOPN);
    }

    @GetMapping("/funnel")
    public SseEmitter funnel() {
        return broadcaster.subscribe(AnalyticsStreams.FUNNEL);
    }

    @GetMapping("/alerts")
    public SseEmitter alerts() {
        return broadcaster.subscribe(AnalyticsStreams.ALERTS);
    }
}

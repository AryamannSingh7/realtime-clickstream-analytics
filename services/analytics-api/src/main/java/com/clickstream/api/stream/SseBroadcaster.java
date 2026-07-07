package com.clickstream.api.stream;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Fans a named stream of events out to every browser currently subscribed to it over SSE.
 *
 * <p>One {@link SseEmitter} set per stream key ({@code metrics}/{@code topn}/{@code funnel}/
 * {@code alerts}). Kafka listener threads call {@link #publish}; MVC request threads call
 * {@link #subscribe}; both touch the same concurrent sets, so emitters live in
 * {@link ConcurrentHashMap#newKeySet() lock-free sets} and dead ones are pruned on the next
 * send. A periodic comment heartbeat keeps idle connections (and any intermediary proxies)
 * from timing the stream out.
 */
@Component
public class SseBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(SseBroadcaster.class);
    private static final long EMITTER_TIMEOUT_MS = Duration.ofMinutes(30).toMillis();

    private final Map<String, Set<SseEmitter>> streams = new ConcurrentHashMap<>();

    /** Open a new SSE subscription to {@code stream}. The caller returns this from an MVC method. */
    public SseEmitter subscribe(String stream) {
        return add(stream, new SseEmitter(EMITTER_TIMEOUT_MS));
    }

    /** Register an emitter under {@code stream} and wire its lifecycle cleanup. Package-private for tests. */
    SseEmitter add(String stream, SseEmitter emitter) {
        Set<SseEmitter> set = streams.computeIfAbsent(stream, s -> ConcurrentHashMap.newKeySet());
        set.add(emitter);
        emitter.onCompletion(() -> set.remove(emitter));
        emitter.onTimeout(() -> {
            set.remove(emitter);
            emitter.complete();
        });
        emitter.onError(e -> set.remove(emitter));
        return emitter;
    }

    /** Push {@code payload} (as a named JSON SSE event) to every subscriber of {@code stream}. */
    public void publish(String stream, Object payload) {
        Set<SseEmitter> set = streams.get(stream);
        if (set == null || set.isEmpty()) {
            return;
        }
        for (SseEmitter emitter : set) {
            try {
                emitter.send(SseEmitter.event().name(stream).data(payload));
            } catch (IOException | IllegalStateException e) {
                // Client went away (or the emitter already completed) — drop it.
                set.remove(emitter);
            }
        }
    }

    /** Number of live subscribers on a stream (used by tests / metrics). */
    public int subscriberCount(String stream) {
        Set<SseEmitter> set = streams.get(stream);
        return set == null ? 0 : set.size();
    }

    /** Keep idle SSE connections and proxies alive with a comment line every 15s. */
    @Scheduled(fixedRate = 15_000)
    public void heartbeat() {
        for (Map.Entry<String, Set<SseEmitter>> entry : streams.entrySet()) {
            Set<SseEmitter> set = entry.getValue();
            for (SseEmitter emitter : set) {
                try {
                    emitter.send(SseEmitter.event().comment("keepalive"));
                } catch (IOException | IllegalStateException e) {
                    set.remove(emitter);
                }
            }
        }
        if (log.isTraceEnabled()) {
            streams.forEach((k, v) -> log.trace("stream {} has {} subscribers", k, v.size()));
        }
    }
}

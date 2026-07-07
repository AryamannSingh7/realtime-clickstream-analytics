package com.clickstream.api.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder;

/** Fan-out and dead-subscriber pruning for {@link SseBroadcaster}, using mocked emitters. */
class SseBroadcasterTest {

    @Test
    void publishesToAllSubscribersOfAStream() throws IOException {
        SseBroadcaster b = new SseBroadcaster();
        SseEmitter a = mock(SseEmitter.class);
        SseEmitter c = mock(SseEmitter.class);
        b.add("metrics", a);
        b.add("metrics", c);
        assertThat(b.subscriberCount("metrics")).isEqualTo(2);

        b.publish("metrics", Map.of("k", "v"));

        verify(a).send(any(SseEventBuilder.class));
        verify(c).send(any(SseEventBuilder.class));
        assertThat(b.subscriberCount("metrics")).isEqualTo(2);
    }

    @Test
    void prunesSubscriberWhoseClientHasGone() throws IOException {
        SseBroadcaster b = new SseBroadcaster();
        SseEmitter live = mock(SseEmitter.class);
        SseEmitter dead = mock(SseEmitter.class);
        doThrow(new IOException("broken pipe")).when(dead).send(any(SseEventBuilder.class));
        b.add("alerts", live);
        b.add("alerts", dead);

        b.publish("alerts", Map.of("x", 1));

        assertThat(b.subscriberCount("alerts")).isEqualTo(1);
        verify(live).send(any(SseEventBuilder.class));
    }

    @Test
    void publishToStreamWithNoSubscribersIsNoop() {
        SseBroadcaster b = new SseBroadcaster();
        assertThatCode(() -> b.publish("funnel", Map.of())).doesNotThrowAnyException();
        assertThat(b.subscriberCount("funnel")).isZero();
    }

    @Test
    void streamsAreIsolatedFromEachOther() throws IOException {
        SseBroadcaster b = new SseBroadcaster();
        SseEmitter metricsSub = mock(SseEmitter.class);
        b.add("metrics", metricsSub);

        b.publish("alerts", Map.of("x", 1));

        verify(metricsSub, org.mockito.Mockito.never()).send(any(SseEventBuilder.class));
        assertThat(b.subscriberCount("metrics")).isEqualTo(1);
    }
}

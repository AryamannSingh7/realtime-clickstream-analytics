package com.clickstream.api.stream;

import com.clickstream.api.stream.StreamDtos.ActiveSessionsView;
import com.clickstream.api.stream.StreamDtos.AlertView;
import com.clickstream.api.stream.StreamDtos.FunnelView;
import com.clickstream.api.stream.StreamDtos.MetricsView;
import com.clickstream.api.stream.StreamDtos.TopNView;
import com.clickstream.avro.ActiveSessionsSnapshot;
import com.clickstream.avro.AnomalyAlert;
import com.clickstream.avro.FunnelSnapshot;
import com.clickstream.avro.MinuteMetrics;
import com.clickstream.avro.PageTopN;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes the {@code analytics.*} Avro output topics and re-broadcasts each record to the
 * matching SSE stream. Each API instance uses a unique consumer group (see {@code
 * application.yml}) so it reads every partition and mirrors the full stream to its clients.
 */
@Component
public class AnalyticsStreamListeners {

    private final SseBroadcaster broadcaster;

    public AnalyticsStreamListeners(SseBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @KafkaListener(topics = AnalyticsStreams.METRICS_TOPIC)
    public void onMetrics(MinuteMetrics metrics) {
        broadcaster.publish(AnalyticsStreams.METRICS, MetricsView.from(metrics));
    }

    @KafkaListener(topics = AnalyticsStreams.TOPN_TOPIC)
    public void onTopN(PageTopN topN) {
        broadcaster.publish(AnalyticsStreams.TOPN, TopNView.from(topN));
    }

    @KafkaListener(topics = AnalyticsStreams.FUNNEL_TOPIC)
    public void onFunnel(FunnelSnapshot snapshot) {
        broadcaster.publish(AnalyticsStreams.FUNNEL, FunnelView.from(snapshot));
    }

    @KafkaListener(topics = AnalyticsStreams.ALERTS_TOPIC)
    public void onAlert(AnomalyAlert alert) {
        broadcaster.publish(AnalyticsStreams.ALERTS, AlertView.from(alert));
    }

    @KafkaListener(topics = AnalyticsStreams.ACTIVE_SESSIONS_TOPIC)
    public void onActiveSessions(ActiveSessionsSnapshot snapshot) {
        broadcaster.publish(AnalyticsStreams.ACTIVE_SESSIONS, ActiveSessionsView.from(snapshot));
    }
}

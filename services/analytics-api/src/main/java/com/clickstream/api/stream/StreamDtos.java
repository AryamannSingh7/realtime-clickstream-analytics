package com.clickstream.api.stream;

import com.clickstream.avro.ActiveSessionsSnapshot;
import com.clickstream.avro.AnomalyAlert;
import com.clickstream.avro.FunnelSnapshot;
import com.clickstream.avro.MinuteMetrics;
import com.clickstream.avro.PageTopN;
import java.time.Instant;
import java.util.List;

/**
 * SSE payloads for the live {@code analytics.*} streams. Deliberately plain records (not the
 * Avro types) so the JSON pushed to browsers is clean and the wire contract is decoupled from
 * the Avro schemas. Each has a {@code from(...)} adapter off its source Avro record.
 */
public final class StreamDtos {

    private StreamDtos() {
    }

    /** Per-minute rollup — {@code analytics.metrics.1m}. */
    public record MetricsView(Instant windowStart, Instant windowEnd, long events, long pageViews,
                              long addToCart, long checkoutStart, long purchases, double revenue) {
        public static MetricsView from(MinuteMetrics m) {
            return new MetricsView(m.getWindowStart(), m.getWindowEnd(), m.getEvents(),
                    m.getPageViews(), m.getAddToCart(), m.getCheckoutStart(), m.getPurchases(),
                    m.getRevenue());
        }
    }

    /** One entry in the top-N pages list. */
    public record PageView(String path, long views) {
    }

    /** Windowed top-N pages — {@code analytics.topn.pages.1m}. */
    public record TopNView(Instant windowStart, Instant windowEnd, List<PageView> pages) {
        public static TopNView from(PageTopN t) {
            List<PageView> pages = t.getEntries().stream()
                    .map(e -> new PageView(e.getPath().toString(), e.getViews()))
                    .toList();
            return new TopNView(t.getWindowStart(), t.getWindowEnd(), pages);
        }
    }

    /** Live conversion-funnel snapshot — {@code analytics.funnel.live}. */
    public record FunnelView(Instant snapshotTime, long views, long addToCart, long checkoutStart,
                             long purchases, double viewToCartRate, double cartToCheckoutRate,
                             double checkoutToPurchaseRate, double overallConversionRate) {
        public static FunnelView from(FunnelSnapshot f) {
            return new FunnelView(f.getSnapshotTime(), f.getViews(), f.getAddToCart(),
                    f.getCheckoutStart(), f.getPurchases(), f.getViewToCartRate(),
                    f.getCartToCheckoutRate(), f.getCheckoutToPurchaseRate(),
                    f.getOverallConversionRate());
        }
    }

    /** Volume anomaly alert — {@code analytics.alerts}. */
    public record AlertView(Instant alertTime, long observed, double baselineMean,
                            double baselineStddev, double zScore, double thresholdK,
                            String direction, long baselineSamples) {
        public static AlertView from(AnomalyAlert a) {
            return new AlertView(a.getAlertTime(), a.getObserved(), a.getBaselineMean(),
                    a.getBaselineStddev(), a.getZScore(), a.getThresholdK(),
                    a.getDirection().toString(), a.getBaselineSamples());
        }
    }

    /** Live active-session count — {@code analytics.active.sessions}. */
    public record ActiveSessionsView(Instant snapshotTime, long activeSessions, long windowSeconds) {
        public static ActiveSessionsView from(ActiveSessionsSnapshot s) {
            return new ActiveSessionsView(
                    s.getSnapshotTime(), s.getActiveSessions(), s.getWindowSeconds());
        }
    }
}

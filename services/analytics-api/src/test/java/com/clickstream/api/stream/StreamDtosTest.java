package com.clickstream.api.stream;

import static org.assertj.core.api.Assertions.assertThat;

import com.clickstream.api.stream.StreamDtos.AlertView;
import com.clickstream.api.stream.StreamDtos.FunnelView;
import com.clickstream.api.stream.StreamDtos.MetricsView;
import com.clickstream.api.stream.StreamDtos.TopNView;
import com.clickstream.avro.AnomalyAlert;
import com.clickstream.avro.FunnelSnapshot;
import com.clickstream.avro.MinuteMetrics;
import com.clickstream.avro.PageCount;
import com.clickstream.avro.PageTopN;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Verifies the Avro -> SSE DTO adapters map every field (and stringify CharSequences). */
class StreamDtosTest {

    private static final Instant T0 = Instant.parse("2026-07-07T10:00:00Z");
    private static final Instant T1 = Instant.parse("2026-07-07T10:01:00Z");

    @Test
    void mapsMinuteMetrics() {
        MinuteMetrics m = MinuteMetrics.newBuilder()
                .setWindowStart(T0).setWindowEnd(T1)
                .setEvents(1000).setPageViews(600).setAddToCart(200)
                .setCheckoutStart(80).setPurchases(40).setRevenue(1234.5)
                .build();

        MetricsView v = MetricsView.from(m);

        assertThat(v.windowStart()).isEqualTo(T0);
        assertThat(v.windowEnd()).isEqualTo(T1);
        assertThat(v.events()).isEqualTo(1000);
        assertThat(v.pageViews()).isEqualTo(600);
        assertThat(v.addToCart()).isEqualTo(200);
        assertThat(v.checkoutStart()).isEqualTo(80);
        assertThat(v.purchases()).isEqualTo(40);
        assertThat(v.revenue()).isEqualTo(1234.5);
    }

    @Test
    void mapsTopNPagesInOrder() {
        PageTopN t = PageTopN.newBuilder()
                .setWindowStart(T0).setWindowEnd(T1)
                .setEntries(List.of(
                        PageCount.newBuilder().setPath("/").setViews(500).build(),
                        PageCount.newBuilder().setPath("/products").setViews(300).build()))
                .build();

        TopNView v = TopNView.from(t);

        assertThat(v.pages()).extracting(StreamDtos.PageView::path).containsExactly("/", "/products");
        assertThat(v.pages()).extracting(StreamDtos.PageView::views).containsExactly(500L, 300L);
    }

    @Test
    void mapsFunnelSnapshot() {
        FunnelSnapshot f = FunnelSnapshot.newBuilder()
                .setSnapshotTime(T0)
                .setViews(1000).setAddToCart(435).setCheckoutStart(241).setPurchases(169)
                .setViewToCartRate(0.435).setCartToCheckoutRate(0.555)
                .setCheckoutToPurchaseRate(0.70).setOverallConversionRate(0.169)
                .build();

        FunnelView v = FunnelView.from(f);

        assertThat(v.snapshotTime()).isEqualTo(T0);
        assertThat(v.views()).isEqualTo(1000);
        assertThat(v.purchases()).isEqualTo(169);
        assertThat(v.viewToCartRate()).isEqualTo(0.435);
        assertThat(v.overallConversionRate()).isEqualTo(0.169);
    }

    @Test
    void mapsAnomalyAlertAndStringifiesDirection() {
        AnomalyAlert a = AnomalyAlert.newBuilder()
                .setAlertTime(T0).setObserved(178509)
                .setBaselineMean(14189.0).setBaselineStddev(3425.0)
                .setZScore(14.37).setThresholdK(3.0)
                .setDirection("spike").setBaselineSamples(8)
                .build();

        AlertView v = AlertView.from(a);

        assertThat(v.alertTime()).isEqualTo(T0);
        assertThat(v.observed()).isEqualTo(178509);
        assertThat(v.zScore()).isEqualTo(14.37);
        assertThat(v.direction()).isEqualTo("spike");
        assertThat(v.baselineSamples()).isEqualTo(8);
    }
}

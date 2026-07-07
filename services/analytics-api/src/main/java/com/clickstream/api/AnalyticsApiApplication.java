package com.clickstream.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Serving layer for the clickstream analytics pipeline.
 *
 * <p>Two responsibilities, added milestone-by-milestone:
 * <ul>
 *   <li><b>Live</b> — consumes the {@code analytics.*} Kafka output topics and fans the
 *       rollups out to browsers over Server-Sent Events (SSE).</li>
 *   <li><b>OLAP</b> — ClickHouse-backed REST endpoints (funnel / unique visitors /
 *       time-series / top pages) over the raw {@code analytics.events} table.</li>
 * </ul>
 *
 * <p>The architectural through-line: Kafka Streams owns low-latency streaming rollups
 * (served live via SSE); ClickHouse owns heavy, ad-hoc OLAP over arbitrary time ranges.
 */
@SpringBootApplication
public class AnalyticsApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsApiApplication.class, args);
    }
}

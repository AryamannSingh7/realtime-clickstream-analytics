package com.clickstream.api.olap;

import java.time.Instant;
import java.util.List;

/**
 * Response payloads for the ClickHouse-backed OLAP endpoints. Grouped here as records so the
 * JSON contract for the dashboard lives in one place.
 */
public final class OlapDtos {

    private OlapDtos() {
    }

    /**
     * One stage of the conversion funnel.
     *
     * @param stage               stage label ({@code view}/{@code cart}/{@code checkout}/{@code purchase})
     * @param visitors            distinct visitors who reached at least this stage
     * @param conversionFromTop   {@code visitors / topStageVisitors} (0..1)
     * @param conversionFromPrev  {@code visitors / previousStageVisitors} (0..1; 1.0 for the top stage)
     */
    public record FunnelStep(String stage, long visitors, double conversionFromTop,
                             double conversionFromPrev) {
    }

    /** Conversion funnel over {@code [from, to)} computed with ClickHouse {@code windowFunnel}. */
    public record FunnelResponse(Instant from, Instant to, int windowSeconds, List<FunnelStep> steps) {
    }

    /** Unique-visitor count in one time bucket ({@code uniqCombined(anonymous_id)}). */
    public record UniqueVisitorsPoint(Instant bucket, long uniqueVisitors) {
    }

    /** Event count for one {@code event_type} in one time bucket. */
    public record TimeseriesPoint(Instant bucket, String eventType, long events) {
    }

    /** A page path and its {@code page_view} count over the range. */
    public record TopPage(String path, long views) {
    }
}

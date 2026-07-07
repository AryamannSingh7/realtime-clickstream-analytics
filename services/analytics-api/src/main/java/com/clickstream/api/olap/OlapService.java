package com.clickstream.api.olap;

import com.clickstream.api.olap.OlapDtos.FunnelResponse;
import com.clickstream.api.olap.OlapDtos.FunnelStep;
import com.clickstream.api.olap.OlapDtos.TimeseriesPoint;
import com.clickstream.api.olap.OlapDtos.TopPage;
import com.clickstream.api.olap.OlapDtos.UniqueVisitorsPoint;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * ClickHouse-backed OLAP queries over the raw {@code analytics.events} table.
 *
 * <p>This is the "heavy, ad-hoc" side of the pipeline: arbitrary time ranges, funnel/retention
 * and high-cardinality unique counts that ClickHouse serves natively ({@code windowFunnel},
 * {@code uniqCombined}). The low-latency streaming roll-ups live on the SSE side instead.
 *
 * <p>Time bounds are always passed as epoch-millis bind parameters and compared via
 * {@code fromUnixTimestamp64Milli(...)} to sidestep any driver/server timezone ambiguity on the
 * {@code DateTime64(3)} column. Only fully-controlled values (an {@code int} window, a
 * {@link TimeBucket} enum's SQL) are ever inlined.
 */
@Service
public class OlapService {

    /** Ordered funnel stages, mirroring the streaming side's {@code FunnelStage}. */
    static final String[] FUNNEL_STAGES = {"view", "cart", "checkout", "purchase"};

    private final JdbcTemplate jdbc;

    public OlapService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Conversion funnel view&rarr;cart&rarr;checkout&rarr;purchase over {@code [from, to)}.
     * A visitor counts toward stage <i>i</i> if they completed steps 1..i within {@code
     * windowSeconds} of their first step (ClickHouse {@code windowFunnel} semantics).
     */
    public FunnelResponse funnel(Instant from, Instant to, int windowSeconds) {
        if (windowSeconds <= 0) {
            throw new IllegalArgumentException("windowSeconds must be positive");
        }
        // windowSeconds is a validated primitive int -> safe to inline into the parametric fn.
        String sql = """
                SELECT level, count() AS visitors
                FROM (
                    SELECT anonymous_id,
                           windowFunnel(%d)(
                               toDateTime(event_time),
                               event_type = 'page_view',
                               event_type = 'add_to_cart',
                               event_type = 'checkout_start',
                               event_type = 'purchase'
                           ) AS level
                    FROM analytics.events
                    WHERE event_time >= fromUnixTimestamp64Milli(?)
                      AND event_time <  fromUnixTimestamp64Milli(?)
                    GROUP BY anonymous_id
                )
                WHERE level > 0
                GROUP BY level
                ORDER BY level
                """.formatted(windowSeconds);

        // atLevel[i] = visitors whose furthest completed step == i (1-based).
        long[] atLevel = new long[FUNNEL_STAGES.length + 1];
        jdbc.query(sql, rs -> {
            int level = rs.getInt("level");
            if (level >= 1 && level <= FUNNEL_STAGES.length) {
                atLevel[level] = rs.getLong("visitors");
            }
        }, from.toEpochMilli(), to.toEpochMilli());

        // reached[i] = visitors who got to *at least* stage i = sum of atLevel[i..n].
        long[] reached = new long[FUNNEL_STAGES.length + 1];
        for (int i = FUNNEL_STAGES.length; i >= 1; i--) {
            reached[i] = atLevel[i] + (i < FUNNEL_STAGES.length ? reached[i + 1] : 0);
        }
        long top = reached[1];

        List<FunnelStep> steps = new ArrayList<>(FUNNEL_STAGES.length);
        for (int i = 1; i <= FUNNEL_STAGES.length; i++) {
            double fromTop = top == 0 ? 0.0 : (double) reached[i] / top;
            double fromPrev = (i == 1) ? 1.0
                    : (reached[i - 1] == 0 ? 0.0 : (double) reached[i] / reached[i - 1]);
            steps.add(new FunnelStep(FUNNEL_STAGES[i - 1], reached[i], fromTop, fromPrev));
        }
        return new FunnelResponse(from, to, windowSeconds, steps);
    }

    /** Unique visitors ({@code uniqCombined(anonymous_id)}) per {@code bucket} over {@code [from, to)}. */
    public List<UniqueVisitorsPoint> uniqueVisitors(Instant from, Instant to, TimeBucket bucket) {
        String sql = """
                SELECT %s AS bucket, uniqCombined(anonymous_id) AS unique_visitors
                FROM analytics.events
                WHERE event_time >= fromUnixTimestamp64Milli(?)
                  AND event_time <  fromUnixTimestamp64Milli(?)
                GROUP BY bucket
                ORDER BY bucket
                """.formatted(bucket.sql());
        return jdbc.query(sql,
                (rs, i) -> new UniqueVisitorsPoint(
                        rs.getTimestamp("bucket").toInstant(),
                        rs.getLong("unique_visitors")),
                from.toEpochMilli(), to.toEpochMilli());
    }

    /**
     * Event counts bucketed by time and {@code event_type} over {@code [from, to)}. When
     * {@code eventType} is non-null the series is restricted to that one type.
     */
    public List<TimeseriesPoint> timeseries(Instant from, Instant to, TimeBucket bucket,
                                            String eventType) {
        boolean filtered = eventType != null && !eventType.isBlank();
        String sql = """
                SELECT %s AS bucket, event_type, count() AS events
                FROM analytics.events
                WHERE event_time >= fromUnixTimestamp64Milli(?)
                  AND event_time <  fromUnixTimestamp64Milli(?)
                  %s
                GROUP BY bucket, event_type
                ORDER BY bucket, event_type
                """.formatted(bucket.sql(), filtered ? "AND event_type = ?" : "");

        Object[] args = filtered
                ? new Object[]{from.toEpochMilli(), to.toEpochMilli(), eventType}
                : new Object[]{from.toEpochMilli(), to.toEpochMilli()};

        return jdbc.query(sql,
                (rs, i) -> new TimeseriesPoint(
                        rs.getTimestamp("bucket").toInstant(),
                        rs.getString("event_type"),
                        rs.getLong("events")),
                args);
    }

    /** Top {@code page_view} paths by count over {@code [from, to)}. */
    public List<TopPage> topPages(Instant from, Instant to, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        String sql = """
                SELECT path, count() AS views
                FROM analytics.events
                WHERE event_type = 'page_view'
                  AND event_time >= fromUnixTimestamp64Milli(?)
                  AND event_time <  fromUnixTimestamp64Milli(?)
                GROUP BY path
                ORDER BY views DESC, path
                LIMIT ?
                """;
        return jdbc.query(sql,
                (rs, i) -> new TopPage(rs.getString("path"), rs.getLong("views")),
                from.toEpochMilli(), to.toEpochMilli(), limit);
    }
}

package com.clickstream.api.olap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.clickstream.api.olap.OlapDtos.FunnelResponse;
import com.clickstream.api.olap.OlapDtos.FunnelStep;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

/**
 * Unit tests for the funnel cumulative math and input validation. ClickHouse is mocked out via
 * a stubbed {@link JdbcTemplate}, so these are deterministic and broker-less.
 */
class OlapServiceTest {

    private static final Instant FROM = Instant.parse("2026-07-07T00:00:00Z");
    private static final Instant TO = Instant.parse("2026-07-07T12:00:00Z");

    /** Feed windowFunnel level rows and assert the reached-at-least-stage counts and rates. */
    @Test
    void funnelCumulatesLevelsAndRates() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        // atLevel: 600 stop at view, 200 at cart, 100 at checkout, 100 complete
        //   -> reached: view 1000, cart 400, checkout 200, purchase 100
        stubFunnelRows(jdbc, new long[][]{{1, 600}, {2, 200}, {3, 100}, {4, 100}});

        FunnelResponse resp = new OlapService(jdbc).funnel(FROM, TO, 1800);

        assertThat(resp.windowSeconds()).isEqualTo(1800);
        List<FunnelStep> steps = resp.steps();
        assertThat(steps).extracting(FunnelStep::stage)
                .containsExactly("view", "cart", "checkout", "purchase");
        assertThat(steps).extracting(FunnelStep::visitors)
                .containsExactly(1000L, 400L, 200L, 100L);

        assertThat(steps.get(0).conversionFromTop()).isEqualTo(1.0);
        assertThat(steps.get(1).conversionFromTop()).isEqualTo(0.4);
        assertThat(steps.get(3).conversionFromTop()).isEqualTo(0.1);

        assertThat(steps.get(0).conversionFromPrev()).isEqualTo(1.0);
        assertThat(steps.get(1).conversionFromPrev()).isEqualTo(0.4);
        assertThat(steps.get(2).conversionFromPrev()).isEqualTo(0.5);
        assertThat(steps.get(3).conversionFromPrev()).isEqualTo(0.5);
    }

    /** An empty range yields zero visitors and zero rates without dividing by zero. */
    @Test
    void funnelHandlesNoData() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        stubFunnelRows(jdbc, new long[][]{});

        FunnelResponse resp = new OlapService(jdbc).funnel(FROM, TO, 1800);

        assertThat(resp.steps()).extracting(FunnelStep::visitors)
                .containsExactly(0L, 0L, 0L, 0L);
        assertThat(resp.steps().get(1).conversionFromTop()).isEqualTo(0.0);
        // Stage 1's "from previous" is defined as 1.0 by convention (it is the entry stage).
        assertThat(resp.steps().get(0).conversionFromPrev()).isEqualTo(1.0);
    }

    @Test
    void funnelRejectsNonPositiveWindow() {
        OlapService svc = new OlapService(mock(JdbcTemplate.class));
        assertThatThrownBy(() -> svc.funnel(FROM, TO, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void topPagesRejectsNonPositiveLimit() {
        OlapService svc = new OlapService(mock(JdbcTemplate.class));
        assertThatThrownBy(() -> svc.topPages(FROM, TO, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Stub {@code jdbc.query(sql, RowCallbackHandler, args...)} to replay the given level rows. */
    private static void stubFunnelRows(JdbcTemplate jdbc, long[][] rows) {
        doAnswer(inv -> {
            RowCallbackHandler rch = inv.getArgument(1);
            for (long[] row : rows) {
                rch.processRow(levelRow((int) row[0], row[1]));
            }
            return null;
        }).when(jdbc).query(anyString(), any(RowCallbackHandler.class), any(), any());
    }

    private static ResultSet levelRow(int level, long visitors) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getInt("level")).thenReturn(level);
        when(rs.getLong("visitors")).thenReturn(visitors);
        return rs;
    }
}

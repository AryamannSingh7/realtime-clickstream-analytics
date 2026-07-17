package com.clickstream.api.olap;

import static org.assertj.core.api.Assertions.assertThat;

import com.clickstream.api.olap.OlapDtos.FunnelResponse;
import com.clickstream.api.olap.OlapDtos.FunnelStep;
import com.clickstream.api.olap.OlapDtos.TimeseriesPoint;
import com.clickstream.api.olap.OlapDtos.TopPage;
import com.clickstream.api.olap.OlapDtos.UniqueVisitorsPoint;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test for {@link OlapService} against a <em>real</em> ClickHouse (Testcontainers),
 * using the actual {@code infra/clickhouse/init/01-schema.sql} DDL. The unit test mocks the
 * {@code JdbcTemplate}, so it can't catch SQL that's wrong against a real server; this validates
 * the {@code windowFunnel} / {@code uniqCombined} queries produce the expected results over a
 * known fixture.
 */
@Testcontainers
class OlapIntegrationIT {

    /** Matches the pinned server version used by the compose stack. */
    private static final DockerImageName CLICKHOUSE_IMAGE =
            DockerImageName.parse("clickhouse/clickhouse-server:24.8");

    @Container
    static final ClickHouseContainer CLICKHOUSE =
            new ClickHouseContainer(CLICKHOUSE_IMAGE)
                    .withUsername("clickstream")
                    .withPassword("clickstream")
                    .withDatabaseName("analytics");

    // Fixture time window: the start of the current hour, so events are recent enough to survive
    // the table's `TTL event_time + 30 DAY` (a fixed past date would be swept immediately) and all
    // land in a single hourly bucket. All events fall inside [FROM, TO).
    private static final Instant FROM = Instant.now().truncatedTo(ChronoUnit.HOURS);
    private static final Instant TO = FROM.plusSeconds(3600);

    private static OlapService service;

    @BeforeAll
    static void loadFixture() throws Exception {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                CLICKHOUSE.getJdbcUrl(), "clickstream", "clickstream");
        dataSource.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        applyRealSchema(jdbc);
        insertFixture(jdbc);
        service = new OlapService(jdbc);
    }

    /**
     * Funnel over the fixture: A completes all 4 stages, B stops at checkout, C at cart, D and E
     * bounce after a page_view. Reached counts: view 5, cart 3, checkout 2, purchase 1.
     */
    @Test
    void funnelMatchesWindowFunnelOverFixture() {
        FunnelResponse response = service.funnel(FROM, TO, 1800);

        List<FunnelStep> steps = response.steps();
        assertThat(steps).extracting(FunnelStep::stage)
                .containsExactly("view", "cart", "checkout", "purchase");
        assertThat(steps).extracting(FunnelStep::visitors)
                .containsExactly(5L, 3L, 2L, 1L);

        assertThat(steps.get(0).conversionFromTop()).isEqualTo(1.0);
        assertThat(steps.get(1).conversionFromTop()).isEqualTo(3.0 / 5.0);
        assertThat(steps.get(2).conversionFromTop()).isEqualTo(2.0 / 5.0);
        assertThat(steps.get(3).conversionFromTop()).isEqualTo(1.0 / 5.0);

        assertThat(steps.get(1).conversionFromPrev()).isEqualTo(3.0 / 5.0);
        assertThat(steps.get(2).conversionFromPrev()).isEqualTo(2.0 / 3.0);
        assertThat(steps.get(3).conversionFromPrev()).isEqualTo(1.0 / 2.0);
    }

    @Test
    void uniqueVisitorsCountsDistinctAnonymousIds() {
        List<UniqueVisitorsPoint> points = service.uniqueVisitors(FROM, TO, TimeBucket.HOUR);

        // Five distinct visitors, all within the single hourly bucket.
        assertThat(points).hasSize(1);
        assertThat(points.get(0).uniqueVisitors()).isEqualTo(5L);
    }

    @Test
    void topPagesRanksPageViewPathsByCount() {
        List<TopPage> pages = service.topPages(FROM, TO, 10);

        assertThat(pages).extracting(TopPage::path).containsExactly("/", "/products");
        assertThat(pages).extracting(TopPage::views).containsExactly(3L, 2L);
    }

    @Test
    void timeseriesCountsEventsByTypePerBucket() {
        List<TimeseriesPoint> all = service.timeseries(FROM, TO, TimeBucket.HOUR, null);
        assertThat(all).extracting(TimeseriesPoint::eventType)
                .containsExactly("add_to_cart", "checkout_start", "page_view", "purchase");
        assertThat(all).extracting(TimeseriesPoint::events)
                .containsExactly(3L, 2L, 5L, 1L);

        List<TimeseriesPoint> purchasesOnly = service.timeseries(FROM, TO, TimeBucket.HOUR, "purchase");
        assertThat(purchasesOnly).hasSize(1);
        assertThat(purchasesOnly.get(0).events()).isEqualTo(1L);
    }

    /** Apply the checked-in production DDL verbatim so the test can't drift from the real schema. */
    private static void applyRealSchema(JdbcTemplate jdbc) throws Exception {
        Path schemaPath = Path.of("..", "..", "infra", "clickhouse", "init", "01-schema.sql");
        String ddl = Files.readString(schemaPath).lines()
                .filter(line -> !line.stripLeading().startsWith("--"))
                .collect(Collectors.joining("\n"));
        for (String statement : ddl.split(";")) {
            if (!statement.isBlank()) {
                jdbc.execute(statement);
            }
        }
    }

    /**
     * Insert the fixture as a single literal INSERT. {@code event_time} is written via
     * {@code fromUnixTimestamp64Milli(...)} — the exact form the queries bound on — so insert and
     * query share identical epoch semantics with no DateTime64 timezone ambiguity.
     */
    private static void insertFixture(JdbcTemplate jdbc) {
        List<String> rows = new ArrayList<>();
        // Three visitors who progress to different depths, plus two single-page bounces.
        addJourney(rows, "anon-a", true, true, true);   // view, cart, checkout, purchase
        addJourney(rows, "anon-b", true, true, false);  // view, cart, checkout
        addJourney(rows, "anon-c", true, false, false); // view, cart
        addPageView(rows, "anon-d", "/products");
        addPageView(rows, "anon-e", "/products");

        jdbc.execute("INSERT INTO analytics.events "
                + "(event_id, event_type, anonymous_id, event_time, path, "
                + "device_type, device_os, device_browser, geo_country, geo_city) VALUES "
                + String.join(",\n", rows));
    }

    private static void addJourney(List<String> rows, String anon,
                                   boolean cart, boolean checkout, boolean purchase) {
        rows.add(row(anon, "page_view", "/", 0));
        if (cart) {
            rows.add(row(anon, "add_to_cart", "/cart", 1_000));
        }
        if (checkout) {
            rows.add(row(anon, "checkout_start", "/checkout", 2_000));
        }
        if (purchase) {
            rows.add(row(anon, "purchase", "/thank-you", 3_000));
        }
    }

    private static void addPageView(List<String> rows, String anon, String path) {
        rows.add(row(anon, "page_view", path, 0));
    }

    private static String row(String anon, String type, String path, long offsetMillis) {
        long eventMillis = FROM.toEpochMilli() + offsetMillis;
        return String.format(
                "('%s','%s','%s',fromUnixTimestamp64Milli(%d),'%s','desktop','macos','chrome','US','NYC')",
                anon + "-" + type, type, anon, eventMillis, path);
    }
}

package com.clickstream.api.olap;

import com.clickstream.api.olap.OlapDtos.FunnelResponse;
import com.clickstream.api.olap.OlapDtos.TimeseriesPoint;
import com.clickstream.api.olap.OlapDtos.TopPage;
import com.clickstream.api.olap.OlapDtos.UniqueVisitorsPoint;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * ClickHouse-backed OLAP endpoints (the "heavy, arbitrary-range" query surface).
 *
 * <p>Time bounds are optional ISO-8601 instants (e.g. {@code 2026-07-07T00:00:00Z}); when
 * omitted they default to the last 24 hours. All endpoints are read-only {@code GET}s.
 */
@RestController
@RequestMapping("/api/olap")
public class OlapController {

    private static final Duration DEFAULT_LOOKBACK = Duration.ofHours(24);

    private final OlapService olap;

    public OlapController(OlapService olap) {
        this.olap = olap;
    }

    @GetMapping("/funnel")
    public FunnelResponse funnel(@RequestParam(required = false) String from,
                                 @RequestParam(required = false) String to,
                                 @RequestParam(defaultValue = "1800") int windowSeconds) {
        Instant[] range = resolveRange(from, to);
        return olap.funnel(range[0], range[1], windowSeconds);
    }

    @GetMapping("/unique-users")
    public List<UniqueVisitorsPoint> uniqueUsers(@RequestParam(required = false) String from,
                                                 @RequestParam(required = false) String to,
                                                 @RequestParam(required = false) String interval) {
        Instant[] range = resolveRange(from, to);
        return olap.uniqueVisitors(range[0], range[1], TimeBucket.from(interval));
    }

    @GetMapping("/timeseries")
    public List<TimeseriesPoint> timeseries(@RequestParam(required = false) String from,
                                            @RequestParam(required = false) String to,
                                            @RequestParam(required = false) String interval,
                                            @RequestParam(required = false) String eventType) {
        Instant[] range = resolveRange(from, to);
        return olap.timeseries(range[0], range[1], TimeBucket.from(interval), eventType);
    }

    @GetMapping("/top-pages")
    public List<TopPage> topPages(@RequestParam(required = false) String from,
                                  @RequestParam(required = false) String to,
                                  @RequestParam(defaultValue = "10") int limit) {
        Instant[] range = resolveRange(from, to);
        return olap.topPages(range[0], range[1], limit);
    }

    /** Resolve {@code from}/{@code to} to a validated half-open range, defaulting to the last 24h. */
    private Instant[] resolveRange(String from, String to) {
        Instant toInstant = parse(to, "to").orElseGet(Instant::now);
        Instant fromInstant = parse(from, "from").orElseGet(() -> toInstant.minus(DEFAULT_LOOKBACK));
        if (!fromInstant.isBefore(toInstant)) {
            throw new IllegalArgumentException("'from' must be strictly before 'to'");
        }
        return new Instant[]{fromInstant, toInstant};
    }

    private static java.util.Optional<Instant> parse(String value, String name) {
        if (value == null || value.isBlank()) {
            return java.util.Optional.empty();
        }
        try {
            return java.util.Optional.of(Instant.parse(value.trim()));
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    "Invalid '" + name + "' timestamp: '" + value + "' (expected ISO-8601, e.g. 2026-07-07T00:00:00Z)");
        }
    }

    /** Bad request parameters -> 400 with a plain message rather than a 500. */
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public String badRequest(IllegalArgumentException e) {
        return e.getMessage();
    }
}

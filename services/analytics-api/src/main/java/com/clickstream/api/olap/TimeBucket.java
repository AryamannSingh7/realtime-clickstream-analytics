package com.clickstream.api.olap;

/**
 * Allowed time-bucket granularities for OLAP roll-ups. Each maps to a fixed, injection-safe
 * ClickHouse expression that snaps {@code event_time} to the start of its bucket — the values
 * are chosen from this enum (never interpolated from raw user input) so they can be inlined
 * into SQL safely.
 */
public enum TimeBucket {
    MINUTE("toStartOfMinute(event_time)"),
    FIVE_MINUTE("toStartOfInterval(event_time, INTERVAL 5 MINUTE)"),
    HOUR("toStartOfHour(event_time)"),
    DAY("toStartOfDay(event_time)");

    private final String sql;

    TimeBucket(String sql) {
        this.sql = sql;
    }

    /** The bucketing SQL expression (safe to inline). */
    public String sql() {
        return sql;
    }

    /**
     * Parse a request value (case-insensitive; accepts {@code 5m} / {@code 5min} aliases for
     * {@link #FIVE_MINUTE}), defaulting to {@link #HOUR} when blank.
     */
    public static TimeBucket from(String value) {
        if (value == null || value.isBlank()) {
            return HOUR;
        }
        return switch (value.trim().toLowerCase()) {
            case "minute", "1m", "1min" -> MINUTE;
            case "five_minute", "5m", "5min", "5minute" -> FIVE_MINUTE;
            case "hour", "1h" -> HOUR;
            case "day", "1d" -> DAY;
            default -> throw new IllegalArgumentException(
                    "Unknown interval '" + value + "'. Allowed: minute, 5m, hour, day.");
        };
    }
}

package com.clickstream.api.olap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TimeBucketTest {

    @Test
    void defaultsToHourWhenBlank() {
        assertThat(TimeBucket.from(null)).isEqualTo(TimeBucket.HOUR);
        assertThat(TimeBucket.from("  ")).isEqualTo(TimeBucket.HOUR);
    }

    @Test
    void parsesAliasesCaseInsensitively() {
        assertThat(TimeBucket.from("MINUTE")).isEqualTo(TimeBucket.MINUTE);
        assertThat(TimeBucket.from("5m")).isEqualTo(TimeBucket.FIVE_MINUTE);
        assertThat(TimeBucket.from("5min")).isEqualTo(TimeBucket.FIVE_MINUTE);
        assertThat(TimeBucket.from("Hour")).isEqualTo(TimeBucket.HOUR);
        assertThat(TimeBucket.from("day")).isEqualTo(TimeBucket.DAY);
    }

    @Test
    void rejectsUnknownInterval() {
        assertThatThrownBy(() -> TimeBucket.from("fortnight"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void everyBucketHasSql() {
        for (TimeBucket b : TimeBucket.values()) {
            assertThat(b.sql()).contains("event_time");
        }
    }
}

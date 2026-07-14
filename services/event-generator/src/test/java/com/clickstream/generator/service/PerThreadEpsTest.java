package com.clickstream.generator.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class PerThreadEpsTest {

    @Test
    void splitsEvenlyWhenDivisible() {
        assertThat(sharesOf(1000, 4)).containsExactly(250, 250, 250, 250);
    }

    @Test
    void distributesTheRemainderToLeadingThreads() {
        assertThat(sharesOf(203, 4)).containsExactly(51, 51, 51, 50);
    }

    @Test
    void sharesAlwaysSumToTheTargetRate() {
        for (int eps : new int[] {1, 199, 200, 12_345, 100_000}) {
            for (int threads : new int[] {1, 2, 3, 4, 8, 16}) {
                assertThat(IntStream.range(0, threads)
                                .map(i -> LoadGeneratorService.perThreadEps(eps, threads, i))
                                .sum())
                        .as("eps=%d threads=%d", eps, threads)
                        .isEqualTo(eps);
            }
        }
    }

    private static int[] sharesOf(int eps, int threads) {
        return IntStream.range(0, threads)
                .map(i -> LoadGeneratorService.perThreadEps(eps, threads, i))
                .toArray();
    }
}

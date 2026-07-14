package com.clickstream.generator.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RampPlanTest {

    @Test
    void interpolatesLinearlyAndLandsExactlyOnTarget() {
        // 100 → 1000 over 9 steps: +100/step, final step is exact.
        assertThat(RampPlan.epsAt(100, 1000, 9, 1)).isEqualTo(200);
        assertThat(RampPlan.epsAt(100, 1000, 9, 5)).isEqualTo(600);
        assertThat(RampPlan.epsAt(100, 1000, 9, 9)).isEqualTo(1000);
    }

    @Test
    void handlesDownwardRamp() {
        assertThat(RampPlan.epsAt(1000, 200, 8, 1)).isEqualTo(900);
        assertThat(RampPlan.epsAt(1000, 200, 8, 8)).isEqualTo(200);
    }

    @Test
    void clampsStepsAtOrBeyondTheEndToTheTarget() {
        assertThat(RampPlan.epsAt(100, 5000, 1, 1)).isEqualTo(5000);
        assertThat(RampPlan.epsAt(100, 5000, 10, 42)).isEqualTo(5000);
    }

    @Test
    void doesNotOverflowAtHighRates() {
        // (to - from) * step must not overflow int before the divide.
        assertThat(RampPlan.epsAt(0, 100_000, 100, 50)).isEqualTo(50_000);
        assertThat(RampPlan.epsAt(0, 100_000, 100, 100)).isEqualTo(100_000);
    }
}

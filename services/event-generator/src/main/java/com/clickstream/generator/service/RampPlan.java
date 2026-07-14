package com.clickstream.generator.service;

/**
 * Pure helper describing a linear EPS ramp so the stepping arithmetic can be unit
 * tested independently of the scheduler that drives it.
 */
final class RampPlan {

    private RampPlan() {}

    /**
     * The target EPS after {@code step} of {@code steps} evenly-spaced steps of a linear
     * ramp from {@code fromEps} to {@code toEps}. The final step lands exactly on
     * {@code toEps} regardless of rounding.
     */
    static int epsAt(int fromEps, int toEps, int steps, int step) {
        if (steps < 1 || step >= steps) {
            return toEps;
        }
        long delta = (long) (toEps - fromEps) * step;
        return fromEps + (int) (delta / steps);
    }
}

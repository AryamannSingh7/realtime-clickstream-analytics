package com.clickstream.generator.service;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Drives scripted load profiles on top of the {@link LoadGeneratorService}'s runtime
 * target rate: a linear <em>ramp</em> between two rates over a duration, and a
 * <em>burst</em> that jumps to a rate, holds it, then restores the previous rate.
 *
 * <p>These give benchmarks repeatable, hands-off load shapes (warm-up ramp, spike to
 * trip the anomaly detector, ramp-to-ceiling throughput sweeps) instead of a human
 * poking {@code POST /rate} at the right moments. A single-threaded scheduler applies
 * the steps; starting a new profile cancels any in-flight one.
 */
@Component
public class RampController {

    private static final Logger log = LoggerFactory.getLogger(RampController.class);

    /** How often the ramp adjusts the target rate while interpolating. */
    static final int STEP_SECONDS = 1;

    private final LoadGeneratorService generator;
    private final ScheduledExecutorService scheduler;

    private ScheduledFuture<?> active;
    private volatile String activeProfile;

    public RampController(LoadGeneratorService generator) {
        this.generator = generator;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "eps-ramp");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Linearly move the target rate from {@code fromEps} to {@code toEps} over
     * {@code durationSeconds}, then hold at {@code toEps}.
     */
    public synchronized void ramp(int fromEps, int toEps, int durationSeconds) {
        cancel();
        int steps = Math.max(1, durationSeconds / STEP_SECONDS);
        generator.setTargetEps(fromEps);
        activeProfile = "ramp " + fromEps + "→" + toEps + " over " + durationSeconds + "s";
        log.info("Starting {}", activeProfile);
        AtomicInteger step = new AtomicInteger();
        active = scheduler.scheduleAtFixedRate(() -> {
            int s = step.incrementAndGet();
            generator.setTargetEps(RampPlan.epsAt(fromEps, toEps, steps, s));
            if (s >= steps) {
                complete();
            }
        }, STEP_SECONDS, STEP_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Jump the target rate to {@code toEps}, hold for {@code durationSeconds}, then
     * restore whatever rate was in effect beforehand.
     */
    public synchronized void burst(int toEps, int durationSeconds) {
        cancel();
        int previous = generator.getTargetEps();
        generator.setTargetEps(toEps);
        activeProfile = "burst " + toEps + " for " + durationSeconds + "s (restore " + previous + ")";
        log.info("Starting {}", activeProfile);
        active = scheduler.schedule(() -> {
            generator.setTargetEps(previous);
            complete();
        }, durationSeconds, TimeUnit.SECONDS);
    }

    /** Cancel any in-flight profile; leaves the current target rate untouched. */
    public synchronized void cancel() {
        if (active != null) {
            active.cancel(false);
            active = null;
        }
        activeProfile = null;
    }

    /** @return a description of the running profile, or {@code null} when idle. */
    public String activeProfile() {
        return activeProfile;
    }

    private synchronized void complete() {
        if (active != null) {
            active.cancel(false);
            active = null;
        }
        log.info("Profile complete: {}", activeProfile);
        activeProfile = null;
    }

    @PreDestroy
    void shutdown() {
        scheduler.shutdownNow();
    }
}

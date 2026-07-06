package com.clickstream.processor.topology;

import com.clickstream.avro.AnomalyAlert;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

/**
 * M4 — EWMA spike/drop detector over whole-stream event volume.
 *
 * <p>All source events are fan-in to a single task (keyed under one constant key), so this
 * processor sees the entire stream. Each incoming record adds to the current bucket's running
 * count. A {@link #BUCKET_INTERVAL} wall-clock punctuator closes the bucket: it scores the
 * observed count against an exponentially weighted moving baseline (mean and variance updated
 * with smoothing factor {@link #ALPHA}), folds the observation into that baseline, and emits an
 * {@link AnomalyAlert} to {@link StreamTopics#ALERTS} when the deviation reaches
 * {@link #THRESHOLD_K} standard deviations in either direction.
 *
 * <p>The baseline is seeded from the first bucket and only scored once {@link #WARMUP_SAMPLES}
 * buckets have accumulated, so a cold start does not fire spurious alerts. The scoring standard
 * deviation is floored at {@code sqrt(mean)} — the Poisson expectation for count data — which
 * both avoids dividing by zero when the baseline is perfectly flat and keeps a large jump
 * detectable against an otherwise varianceless history.
 */
public class EwmaAnomalyProcessor extends ContextualProcessor<String, Long, String, AnomalyAlert> {

    static final String STORE_NAME = "anomaly-ewma-store";
    static final String OUTPUT_KEY = "volume";

    /** Bucket length and punctuator cadence: one score per minute of wall-clock time. */
    static final Duration BUCKET_INTERVAL = Duration.ofMinutes(1);

    /** EWMA smoothing factor (~9-bucket effective memory). Higher reacts faster, lower is steadier. */
    static final double ALPHA = 0.2;

    /** Standard deviations of deviation required to fire an alert. */
    static final double THRESHOLD_K = 3.0;

    /** Buckets to accumulate before the baseline is trusted enough to score against. */
    static final long WARMUP_SAMPLES = 5;

    private static final String COUNT_KEY = "count";
    private static final String MEAN_KEY = "mean";
    private static final String VAR_KEY = "var";
    private static final String SAMPLES_KEY = "samples";

    private KeyValueStore<String, Double> state;

    @Override
    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, AnomalyAlert> context) {
        super.init(context);
        this.state = context.getStateStore(STORE_NAME);
        context.schedule(BUCKET_INTERVAL, PunctuationType.WALL_CLOCK_TIME, this::closeBucket);
    }

    @Override
    public void process(Record<String, Long> record) {
        state.put(COUNT_KEY, get(COUNT_KEY) + record.value());
    }

    private void closeBucket(long wallClockMs) {
        long observed = (long) get(COUNT_KEY);
        state.put(COUNT_KEY, 0.0); // start the next bucket clean

        long samples = (long) get(SAMPLES_KEY);
        if (samples == 0) {
            // Seed the baseline from the first bucket; nothing to score against yet.
            seedBaseline(observed);
            return;
        }

        double mean = get(MEAN_KEY);
        double stddev = Math.max(Math.sqrt(get(VAR_KEY)), Math.sqrt(Math.max(mean, 1.0)));
        double z = (observed - mean) / stddev;

        if (samples >= WARMUP_SAMPLES && Math.abs(z) >= THRESHOLD_K) {
            AnomalyAlert alert = new AnomalyAlert(
                    Instant.ofEpochMilli(wallClockMs),
                    observed, mean, stddev, z, THRESHOLD_K,
                    observed >= mean ? "spike" : "drop",
                    samples);
            context().forward(new Record<>(OUTPUT_KEY, alert, wallClockMs));
        }

        updateBaseline(observed, mean, samples);
    }

    /** Standard incremental EWMA of mean and variance (West's update). */
    private void updateBaseline(long observed, double mean, long samples) {
        double diff = observed - mean;
        double incr = ALPHA * diff;
        state.put(MEAN_KEY, mean + incr);
        state.put(VAR_KEY, (1.0 - ALPHA) * (get(VAR_KEY) + diff * incr));
        state.put(SAMPLES_KEY, (double) (samples + 1));
    }

    private void seedBaseline(long observed) {
        state.put(MEAN_KEY, (double) observed);
        state.put(VAR_KEY, 0.0);
        state.put(SAMPLES_KEY, 1.0);
    }

    private double get(String key) {
        Double value = state.get(key);
        return value == null ? 0.0 : value;
    }
}

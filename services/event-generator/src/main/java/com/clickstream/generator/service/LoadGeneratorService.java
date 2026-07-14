package com.clickstream.generator.service;

import com.clickstream.avro.ClickEvent;
import com.clickstream.avro.Device;
import com.clickstream.avro.EventType;
import com.clickstream.avro.Geo;
import com.clickstream.generator.config.GeneratorProperties;
import com.clickstream.generator.journey.EventTemplate;
import com.clickstream.generator.journey.JourneySimulator;
import com.clickstream.generator.journey.UserSession;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Single-threaded, rate-paced producer of synthetic {@link ClickEvent}s.
 *
 * <p>A fixed pool of {@link UserSession}s is walked: on each emit a random session
 * advances one step of its pre-built journey; finished sessions are transparently
 * recycled into brand-new visits. Throughput is held near the configured target by
 * emitting in small batches ({@value #BATCHES_PER_SEC}/s) and parking the remainder
 * of each batch window, which keeps {@code parkNanos} overhead negligible even at
 * thousands of events per second. Rate, pause and resume are adjustable at runtime.
 *
 * <p>For high-throughput load tests the work is spread across {@code emitterThreads}
 * parallel producers: the target rate is divided evenly between them and each thread
 * owns a disjoint slice of the session pool, so the threads never touch shared mutable
 * session state (the {@link KafkaTemplate} producer is itself thread-safe and shared).
 */
@Service
public class LoadGeneratorService {

    private static final Logger log = LoggerFactory.getLogger(LoadGeneratorService.class);

    private static final int BATCHES_PER_SEC = 200;
    private static final long BATCH_INTERVAL_NANOS = 1_000_000_000L / BATCHES_PER_SEC;

    private final GeneratorProperties props;
    private final JourneySimulator simulator;
    private final KafkaTemplate<String, ClickEvent> kafkaTemplate;
    private final MeterRegistry meters;

    private final AtomicLong totalProduced = new AtomicLong();
    private final AtomicLong sendErrors = new AtomicLong();
    private final Map<EventType, Counter> eventCounters = new EnumMap<>(EventType.class);

    private volatile int targetEps;
    private volatile boolean paused = false;
    private volatile boolean running = false;

    private UserSession[] pool;
    private Thread[] emitters;

    public LoadGeneratorService(
            GeneratorProperties props,
            JourneySimulator simulator,
            KafkaTemplate<String, ClickEvent> kafkaTemplate,
            MeterRegistry meters) {
        this.props = props;
        this.simulator = simulator;
        this.kafkaTemplate = kafkaTemplate;
        this.meters = meters;
        this.targetEps = props.eps();
    }

    @PostConstruct
    void start() {
        pool = new UserSession[props.virtualUsers()];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = simulator.newSession();
        }
        for (EventType type : EventType.values()) {
            eventCounters.put(type, Counter.builder("generator.events.produced")
                    .description("ClickEvents produced to Kafka, by type")
                    .tag("event_type", type.name())
                    .register(meters));
        }
        Gauge.builder("generator.target.eps", this, LoadGeneratorService::getTargetEps)
                .description("Configured target events per second")
                .register(meters);
        Gauge.builder("generator.virtual.users", this, s -> s.pool.length)
                .description("Size of the simulated user-session pool")
                .register(meters);

        // Never spin up more emitter threads than there are pool slots to divide
        // among them, so every thread owns at least one session.
        int threads = Math.max(1, Math.min(props.emitterThreads(), pool.length));
        Gauge.builder("generator.emitter.threads", this, s -> s.emitters.length)
                .description("Number of parallel emitter threads")
                .register(meters);

        running = true;
        emitters = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            int per = pool.length / threads;
            int rem = pool.length % threads;
            int start = t * per + Math.min(t, rem);
            int end = start + per + (t < rem ? 1 : 0);
            int threadIndex = t;
            Thread emitter = new Thread(
                    () -> runLoop(start, end, threadIndex, threads), "event-emitter-" + t);
            emitter.setDaemon(true);
            emitters[t] = emitter;
            emitter.start();
        }
        log.info("Event generator started: targetEps={}, virtualUsers={}, emitterThreads={}, topic={}",
                targetEps, pool.length, threads, props.topic());
    }

    @PreDestroy
    void stop() {
        running = false;
        if (emitters != null) {
            for (Thread emitter : emitters) {
                emitter.interrupt();
            }
        }
        log.info("Event generator stopped after producing {} events ({} send errors)",
                totalProduced.get(), sendErrors.get());
    }

    /**
     * Rate-paced emit loop for one emitter thread, driving the pool slice
     * {@code [start, end)} at this thread's share of the global target rate.
     */
    private void runLoop(int start, int end, int threadIndex, int numThreads) {
        // Deficit-based pacing: each wake-up emits however many events this thread's share
        // of the target rate says it owes since a baseline. This self-corrects for coarse OS
        // timers (notably Windows' ~15ms tick, which makes a fixed per-batch park undershoot
        // badly) so the average throughput tracks the target. The baseline is rebased on
        // pause/resume and whenever the rate changes, so neither inflates a catch-up burst.
        // Pacing uses a thread-local produced count (not the shared total) so each thread
        // meters its own slice independently.
        long baseNanos = System.nanoTime();
        long produced = 0;
        long baseCount = 0;
        int pacedTarget = targetEps;
        int myEps = perThreadEps(pacedTarget, numThreads, threadIndex);

        while (running) {
            if (paused) {
                LockSupport.parkNanos(BATCH_INTERVAL_NANOS);
                baseNanos = System.nanoTime();
                baseCount = produced;
                continue;
            }
            if (targetEps != pacedTarget) {
                pacedTarget = targetEps;
                myEps = perThreadEps(pacedTarget, numThreads, threadIndex);
                baseNanos = System.nanoTime();
                baseCount = produced;
            }

            double elapsedSec = (System.nanoTime() - baseNanos) / 1_000_000_000.0;
            long shouldHave = (long) (elapsedSec * myEps);
            long deficit = shouldHave - (produced - baseCount);
            // Cap a single wake-up's burst at one second of events so a long GC pause or
            // a stalled broker can't trigger an unbounded flood once things recover.
            long burst = Math.min(Math.max(deficit, 0L), Math.max(myEps, 1));
            for (long i = 0; i < burst && running && !paused && targetEps == pacedTarget; i++) {
                emitOne(start, end);
                produced++;
            }
            LockSupport.parkNanos(BATCH_INTERVAL_NANOS);
        }
    }

    /** This thread's whole-number share of {@code totalEps}, distributing the remainder. */
    static int perThreadEps(int totalEps, int numThreads, int threadIndex) {
        return totalEps / numThreads + (threadIndex < totalEps % numThreads ? 1 : 0);
    }

    private void emitOne(int start, int end) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int idx = start + rnd.nextInt(end - start);
        UserSession session = pool[idx];
        EventTemplate template = session.nextTemplate();
        if (template == null) {
            session = simulator.newSession();
            pool[idx] = session;
            template = session.nextTemplate();
            if (template == null) {
                return; // empty journey (shouldn't happen) — skip this tick
            }
        }

        ClickEvent event = toEvent(session, template, rnd);
        try {
            kafkaTemplate.send(props.topic(), event.getAnonymousId(), event);
            totalProduced.incrementAndGet();
            Counter c = eventCounters.get(template.type());
            if (c != null) {
                c.increment();
            }
        } catch (RuntimeException ex) {
            // Buffer-full / serialization errors shouldn't kill the emitter thread.
            if (sendErrors.incrementAndGet() % 1000 == 1) {
                log.warn("Failed to enqueue event to Kafka (suppressing further warnings)", ex);
            }
        }
    }

    private ClickEvent toEvent(UserSession session, EventTemplate t, ThreadLocalRandom rnd) {
        Instant eventTime = Instant.now();
        if (props.lateEventMaxDelayMs() > 0 && rnd.nextDouble() < props.lateEventRatio()) {
            eventTime = eventTime.minusMillis(rnd.nextLong(props.lateEventMaxDelayMs() + 1));
        }

        return ClickEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(t.type())
                .setAnonymousId(session.anonymousId())
                .setUserId(session.userId())
                .setEventTime(eventTime)
                .setPath(t.path())
                .setReferrer(session.referrer())
                .setUtmSource(session.utmSource())
                .setUtmCampaign(session.utmCampaign())
                .setProductId(t.productId())
                .setPrice(t.price())
                .setRevenue(t.revenue())
                .setDevice(Device.newBuilder()
                        .setType(session.device().type())
                        .setOs(session.device().os())
                        .setBrowser(session.device().browser())
                        .build())
                .setGeo(Geo.newBuilder()
                        .setCountry(session.geo().country())
                        .setCity(session.geo().city())
                        .build())
                .setUserAgent(session.userAgent())
                .build();
    }

    // --- runtime control / status (used by the web layer) ---

    public void setTargetEps(int eps) {
        this.targetEps = Math.max(1, eps);
        log.info("Target EPS changed to {}", this.targetEps);
    }

    public int getTargetEps() {
        return targetEps;
    }

    public void pause() {
        this.paused = true;
        log.info("Event generator paused");
    }

    public void resume() {
        this.paused = false;
        log.info("Event generator resumed");
    }

    public boolean isPaused() {
        return paused;
    }

    public boolean isRunning() {
        return running;
    }

    public long getTotalProduced() {
        return totalProduced.get();
    }

    public long getSendErrors() {
        return sendErrors.get();
    }

    public int getVirtualUsers() {
        return pool != null ? pool.length : 0;
    }

    public int getEmitterThreads() {
        return emitters != null ? emitters.length : 0;
    }

    public String getTopic() {
        return props.topic();
    }
}

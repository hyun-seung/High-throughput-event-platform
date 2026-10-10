package messaging.common.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;

/** Bounded labels only. Nested phase durations overlap and must not be added together. */
public final class ScheduledWorkMetrics {
    public enum Worker { RESULT_INBOX, RESULT_OUTBOX, COMPLETE_CLEANUP }
    public enum Phase { POLL, QUERY, ITEM, HANDOFF, LOAD, STEP_QUERY, TTL, DELETE }
    private final MeterRegistry registry;
    private final EnumMap<Phase, Timer> timers = new EnumMap<>(Phase.class);
    private final Timer idle;
    private final Timer dueAge;
    private final DistributionSummary candidates;
    private final DistributionSummary eligible;
    private volatile Long lastPollFinished;

    public ScheduledWorkMetrics(MeterRegistry registry, Worker worker) {
        this.registry = registry;
        String label = worker.name().toLowerCase(java.util.Locale.ROOT);
        Phase[] phases = switch (worker) {
            case RESULT_INBOX -> new Phase[] {Phase.POLL, Phase.QUERY, Phase.ITEM};
            case RESULT_OUTBOX -> new Phase[] {Phase.POLL, Phase.QUERY, Phase.ITEM, Phase.HANDOFF};
            case COMPLETE_CLEANUP -> new Phase[] {Phase.POLL, Phase.QUERY, Phase.ITEM, Phase.LOAD,
                    Phase.STEP_QUERY, Phase.TTL, Phase.DELETE};
        };
        for (Phase phase : phases) {
            timers.put(phase, timer("messaging.scheduled.work.duration", label,
                    "phase", phase.name().toLowerCase(java.util.Locale.ROOT)));
        }
        idle = timer("messaging.scheduled.work.idle", label);
        dueAge = timer("messaging.scheduled.work.due.age", label);
        candidates = DistributionSummary.builder("messaging.scheduled.work.page.items")
                .tags("worker", label, "kind", "candidate").register(registry);
        eligible = DistributionSummary.builder("messaging.scheduled.work.page.items")
                .tags("worker", label, "kind", "eligible").register(registry);
    }

    private Timer timer(String name, String worker, String... tags) {
        return Timer.builder(name).tag("worker", worker).tags(tags)
                .serviceLevelObjectives(Duration.ofMillis(10), Duration.ofMillis(50), Duration.ofMillis(100),
                        Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofSeconds(2),
                        Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofSeconds(20), Duration.ofSeconds(30),
                        Duration.ofSeconds(60), Duration.ofSeconds(120), Duration.ofSeconds(300))
                .register(registry);
    }

    public Scope start(Phase phase) {
        long started = registry.config().clock().monotonicTime();
        Long previous = lastPollFinished;
        if (phase == Phase.POLL && previous != null) idle.record(Math.max(0, started - previous), TimeUnit.NANOSECONDS);
        return new Scope(phase, started);
    }

    public void page(int candidateCount, int eligibleCount) {
        candidates.record(candidateCount);
        eligible.record(eligibleCount);
    }

    /** Due time is the queried candidate's schedule, not an end-to-end message timestamp. */
    public void due(long dueMillis, long nowMillis) {
        dueAge.record(Math.max(0, nowMillis - dueMillis), TimeUnit.MILLISECONDS);
    }

    public final class Scope implements AutoCloseable {
        private final Phase phase;
        private final long started;
        private boolean closed;

        private Scope(Phase phase, long started) { this.phase = phase; this.started = started; }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            long finished = registry.config().clock().monotonicTime();
            timers.get(phase).record(Math.max(0, finished - started), TimeUnit.NANOSECONDS);
            if (phase == Phase.POLL) lastPollFinished = finished;
        }
    }
}

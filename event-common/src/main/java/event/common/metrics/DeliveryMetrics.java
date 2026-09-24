package event.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/** Fixed dimensions only: never put delivery IDs, tenants, payloads or exception messages in tags. */
public final class DeliveryMetrics {
    public enum Stage { API_PUBLISH, INGRESS_PROCESS, INGRESS_STORE, INGRESS_PUBLISH,
        DISPATCH_PROCESS, DISPATCH_CLAIM, DISPATCH_HTTP, DISPATCH_STORE }
    public enum Outcome { INGRESS_FORWARDED, INGRESS_DLT, INGRESS_DLT_FAILED,
        DISPATCH_ACCEPTED, DISPATCH_DUPLICATE, DISPATCH_IN_PROGRESS, DISPATCH_REVIEW,
        DISPATCH_RETRY_SCHEDULED, DISPATCH_RETRY_WAIT, DISPATCH_DECISION_PENDING }

    private final MeterRegistry registry;
    private final EnumMap<Stage, Timer> success = new EnumMap<>(Stage.class);
    private final EnumMap<Stage, Timer> failure = new EnumMap<>(Stage.class);
    private final EnumMap<Stage, AtomicInteger> active = new EnumMap<>(Stage.class);
    private final EnumMap<Outcome, Counter> outcomes = new EnumMap<>(Outcome.class);
    private final Timer acceptanceLatency;
    private final Counter invalidTimestamp;

    public DeliveryMetrics(MeterRegistry registry) {
        this.registry = registry;
        for (Stage stage : Stage.values()) {
            String name = stage.name().toLowerCase(Locale.ROOT);
            success.put(stage, timer(name, "success"));
            failure.put(stage, timer(name, "failure"));
            AtomicInteger current = new AtomicInteger();
            active.put(stage, current);
            Gauge.builder("delivery.stage.active", current, AtomicInteger::get).tag("stage", name).register(registry);
        }
        for (Outcome outcome : Outcome.values()) {
            outcomes.put(outcome, registry.counter("delivery.outcomes", "outcome", outcome.name().toLowerCase(Locale.ROOT)));
        }
        acceptanceLatency = Timer.builder("delivery.acceptance.latency")
                .description("First ingress timestamp to newly persisted provider acceptance; wall clock, not final delivery")
                .register(registry);
        invalidTimestamp = registry.counter("delivery.acceptance.invalid.timestamp");
    }

    private Timer timer(String stage, String result) {
        return Timer.builder("delivery.stage.duration").tags("stage", stage, "result", result).register(registry);
    }

    public <T> T measure(Stage stage, Supplier<T> action) {
        Timer.Sample sample = Timer.start(registry);
        active.get(stage).incrementAndGet();
        boolean succeeded = false;
        try {
            T result = action.get();
            succeeded = true;
            return result;
        } finally {
            finish(stage, sample, succeeded);
        }
    }

    public void measure(Stage stage, Runnable action) {
        measure(stage, () -> { action.run(); return null; });
    }

    /** Includes synchronous send failures and waits for the Kafka acknowledgement, not just submission. */
    public <T> CompletableFuture<T> measureAsync(Stage stage, Supplier<CompletableFuture<T>> action) {
        Timer.Sample sample = Timer.start(registry);
        active.get(stage).incrementAndGet();
        final CompletableFuture<T> future;
        try {
            future = java.util.Objects.requireNonNull(action.get());
        } catch (RuntimeException | Error error) {
            finish(stage, sample, false);
            throw error;
        }
        CompletableFuture<T> measured = new CompletableFuture<>();
        future.whenComplete((result, error) -> {
            finish(stage, sample, error == null);
            // Preserve the original failure for callers that inspect the acknowledgement cause.
            if (error == null) measured.complete(result);
            else measured.completeExceptionally(error);
        });
        return measured;
    }

    private void finish(Stage stage, Timer.Sample sample, boolean succeeded) {
        sample.stop((succeeded ? success : failure).get(stage));
        active.get(stage).decrementAndGet();
    }

    public void outcome(Outcome outcome) { outcomes.get(outcome).increment(); }

    public void accepted(Instant ingress, Instant persisted) {
        if (ingress == null || persisted.isBefore(ingress)) {
            invalidTimestamp.increment();
            return;
        }
        acceptanceLatency.record(Duration.between(ingress, persisted));
    }
}

package messaging.delivery.result.operations;

import messaging.common.recovery.FailureBackoff;
import messaging.common.recovery.StorageFailure;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/** Recheck committed operator intents without creating a new decision or extending a deadline. */
public final class ResolutionRecoveryWorker {
    private final ResolutionOperations operations;
    private final MeterRegistry meters;
    private final FailureBackoff backoff;
    private final int batchSize;

    public ResolutionRecoveryWorker(ResolutionOperations operations, MeterRegistry meters, FailureBackoff backoff, int batchSize) {
        if (batchSize < 1 || batchSize > 20) throw new IllegalArgumentException("Resolution recovery batch size must be 1..20");
        this.operations = operations; this.meters = meters; this.backoff = backoff; this.batchSize = batchSize;
        meters.gauge("delivery.resolution.recovery.delay", backoff, FailureBackoff::remainingSeconds);
        meters.counter("delivery.resolution.recovery", "outcome", "idle");
    }

    @Scheduled(fixedDelayString = "${resolution.recovery.poll-ms:30000}", scheduler = "resolutionRecoveryScheduler")
    public synchronized void tick() {
        if (backoff.blocked()) return;
        for (int i = 0; i < batchSize; i++) {
            var ticket = backoff.acquire();
            if (ticket == null) return;
            try {
                var due = operations.claimDue();
                if (due.isEmpty()) {
                    meters.counter("delivery.resolution.recovery", "outcome", "idle").increment();
                    backoff.succeeded(ticket);
                    return;
                }
                var action = due.get();
                var outcome = operations.resume(action.tenantId(), action.actionId());
                meters.counter("delivery.resolution.recovery", "outcome", outcome.name().toLowerCase(java.util.Locale.ROOT)).increment();
                backoff.succeeded(ticket);
            } catch (RuntimeException failure) {
                if (StorageFailure.unavailable(failure)) backoff.failed(ticket);
                else backoff.abandon(ticket);
                meters.counter("delivery.resolution.recovery", "outcome", "error").increment();
                LoggerFactory.getLogger(ResolutionRecoveryWorker.class).warn("Resolution audit retained for later recheck: failure={}", failure.getClass().getSimpleName());
                return;
            }
        }
    }
}

package event.delivery.result.cleanup;

import event.common.lifecycle.DeliveryCompactor;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

public class CleanupWorker {
    private final CleanupRepository repository;
    private final DeliveryCompactor compactor;
    private final MeterRegistry meters;
    public CleanupWorker(CleanupRepository repository, DeliveryCompactor compactor, MeterRegistry meters) {
        this.repository = repository; this.compactor = compactor; this.meters = meters;
    }
    @Scheduled(fixedDelayString = "${cleanup.poll-ms:1000}", scheduler = "cleanupTaskScheduler")
    public void tick() {
        // Bounded work per tick; SQL lease provides restart and multiple-worker coordination.
        for (int i = 0; i < 20; i++) {
            CleanupRepository.Claim claim = null;
            try {
                var next = repository.claim();
                if (next.isEmpty()) return;
                claim = next.get();
                boolean changed = compactor.compact(claim.result());
                repository.done(claim);
                meters.counter("delivery.cleanup.events", "outcome", changed ? "compacted" : "already_compacted").increment();
            } catch (RuntimeException failure) {
                if (claim != null) {
                    try { repository.retry(claim); }
                    catch (RuntimeException retryFailure) { /* SQL lease remains the recovery record. */ }
                }
                meters.counter("delivery.cleanup.events", "outcome", "retained").increment();
                LoggerFactory.getLogger(CleanupWorker.class).warn("Delivery cleanup retained: eventId={} failure={}",
                        claim == null ? "unclaimed" : claim.result().eventId(), failure.getClass().getSimpleName());
                if (claim == null) return;
            }
        }
    }
}

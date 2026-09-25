package event.delivery.result.cleanup;

import event.common.lifecycle.DeliveryCompactor;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import io.micrometer.core.instrument.Timer;

import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CleanupWorker implements AutoCloseable {
    private final CleanupRepository repository;
    private final DeliveryCompactor compactor;
    private final MeterRegistry meters;
    private final int concurrency;
    private final int batchSize;
    private final ExecutorService workers;
    private final AtomicBoolean closing = new AtomicBoolean();
    private final AtomicInteger active = new AtomicInteger();
    public CleanupWorker(CleanupRepository repository, DeliveryCompactor compactor, MeterRegistry meters) {
        this(repository, compactor, meters, 4, 20);
    }
    public CleanupWorker(CleanupRepository repository, DeliveryCompactor compactor, MeterRegistry meters,
                         int concurrency, int batchSize) {
        if (concurrency < 1 || concurrency > 16 || batchSize < 1 || batchSize > 1000)
            throw new IllegalArgumentException("Cleanup concurrency must be 1..16 and batch size 1..1000");
        this.repository = repository; this.compactor = compactor; this.meters = meters;
        this.concurrency = concurrency; this.batchSize = batchSize;
        workers = Executors.newFixedThreadPool(concurrency, Thread.ofPlatform().daemon().name("cleanup-work-", 0).factory());
        meters.gauge("delivery.cleanup.active", active);
    }
    @Scheduled(fixedDelayString = "${cleanup.poll-ms:1000}", scheduler = "cleanupTaskScheduler")
    public synchronized void tick() {
        if (closing.get()) return;
        // One bounded batch per lane. No preclaimed queue and no overlapping ticks in this instance.
        Callable<Void> batch = () -> { drain(); return null; };
        try {
            for (var task : workers.invokeAll(Collections.nCopies(concurrency, batch))) task.get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); // Unfinished claims remain recoverable through the SQL lease.
        } catch (RejectedExecutionException | CancellationException stopped) {
            if (!closing.get()) throw stopped;
        } catch (ExecutionException failed) {
            throw new IllegalStateException("Cleanup batch failed; SQL claims retained", failed.getCause());
        }
    }

    private void drain() {
        active.incrementAndGet();
        try {
            for (int i = 0; i < batchSize && !closing.get() && !Thread.currentThread().isInterrupted(); i++) {
                if (!processOne()) return;
            }
        } finally { active.decrementAndGet(); }
    }

    private boolean processOne() {
        CleanupRepository.Claim claim = null;
        var timer = Timer.start(meters);
        try {
            var next = repository.claim();
            if (next.isEmpty()) return false;
            claim = next.get();
            boolean changed = compactor.compact(claim.result());
            boolean owned = repository.done(claim);
            meters.counter("delivery.cleanup.events", "outcome", !owned ? "lease_lost"
                    : changed ? "compacted" : "already_compacted").increment();
        } catch (RuntimeException failure) {
            if (claim != null) {
                try { repository.retry(claim); }
                catch (RuntimeException retryFailure) { /* SQL lease remains the recovery record. */ }
            }
            meters.counter("delivery.cleanup.events", "outcome", "retained").increment();
            LoggerFactory.getLogger(CleanupWorker.class).warn("Delivery cleanup retained: eventId={} failure={}",
                    claim == null ? "unclaimed" : claim.result().eventId(), failure.getClass().getSimpleName());
            if (claim == null) return false;
        } finally {
            if (claim != null) timer.stop(meters.timer("delivery.cleanup.duration"));
        }
        return true;
    }

    @Override public void close() {
        closing.set(true);
        workers.shutdown();
        try {
            if (!workers.awaitTermination(10, TimeUnit.SECONDS)) cancelQueued();
        } catch (InterruptedException interrupted) {
            cancelQueued(); Thread.currentThread().interrupt();
        }
    }
    private void cancelQueued() {
        // invokeAll waits on these futures; shutdownNow alone would leave queued futures incomplete.
        for (var task : workers.shutdownNow()) if (task instanceof Future<?> future) future.cancel(false);
    }
}

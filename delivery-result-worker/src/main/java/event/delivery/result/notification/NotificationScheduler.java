package event.delivery.result.notification;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import event.common.recovery.FailureBackoff;
import event.common.recovery.StorageFailure;
import java.time.Duration;

public class NotificationScheduler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(NotificationScheduler.class);
    private final ExecutorService workers;
    private final Semaphore slots;
    private final Set<Long> active = ConcurrentHashMap.newKeySet();
    private final List<Long> customers;
    private final NotificationService service;
    private final MeterRegistry meters;
    private final FailureBackoff backoff;
    private int cursor;

    public NotificationScheduler(NotificationService service, NotificationProperties settings, MeterRegistry meters) {
        this(service, settings, meters, new FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30)));
    }
    public NotificationScheduler(NotificationService service, NotificationProperties settings, MeterRegistry meters, FailureBackoff backoff) {
        this.service = service; this.meters = meters;
        this.backoff = backoff;
        customers = settings.customers().keySet().stream().sorted().toList();
        workers = Executors.newFixedThreadPool(settings.concurrency());
        slots = new Semaphore(settings.concurrency());
        meters.gauge("delivery.notification.active", active, Set::size);
        meters.gauge("delivery.notification.recovery.delay", backoff, FailureBackoff::remainingSeconds);
    }

    @Scheduled(fixedDelayString = "${notification.poll-ms:100}", scheduler = "notificationTaskScheduler")
    public synchronized void tick() {
        for (int i = 0; i < customers.size(); i++) {
            if (!slots.tryAcquire()) return;
            long tenant = customers.get(cursor++ % customers.size());
            if (cursor >= customers.size()) cursor = 0;
            if (!active.add(tenant)) { slots.release(); continue; }
            var ticket = backoff.acquire();
            if (ticket == null) {
                active.remove(tenant); slots.release();
                meters.counter("delivery.notification.events", "outcome", "poll_deferred").increment(); return;
            }
            try {
                workers.submit(() -> {
                    try { service.deliver(tenant); backoff.succeeded(ticket); }
                    catch (RuntimeException failure) {
                        if (StorageFailure.unavailable(failure)) backoff.failed(ticket); else backoff.abandon(ticket);
                        meters.counter("delivery.notification.events", "outcome", "work_failed").increment();
                        log.warn("Customer notification work retained: tenantId={} failure={}", tenant, failure.getClass().getSimpleName());
                    } finally { active.remove(tenant); slots.release(); }
                });
            } catch (RejectedExecutionException closing) { backoff.abandon(ticket); active.remove(tenant); slots.release(); }
        }
    }

    @Override public void close() {
        workers.shutdown();
        try { if (!workers.awaitTermination(10, TimeUnit.SECONDS)) workers.shutdownNow(); }
        catch (InterruptedException e) { workers.shutdownNow(); Thread.currentThread().interrupt(); }
    }
}

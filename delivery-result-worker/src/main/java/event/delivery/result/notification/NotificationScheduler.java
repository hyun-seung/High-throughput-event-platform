package event.delivery.result.notification;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class NotificationScheduler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(NotificationScheduler.class);
    private final ExecutorService workers;
    private final Semaphore slots;
    private final Set<Long> active = ConcurrentHashMap.newKeySet();
    private final List<Long> customers;
    private final NotificationService service;
    private final MeterRegistry meters;
    private int cursor;

    public NotificationScheduler(NotificationService service, NotificationProperties settings, MeterRegistry meters) {
        this.service = service; this.meters = meters;
        customers = settings.customers().keySet().stream().sorted().toList();
        workers = Executors.newFixedThreadPool(settings.concurrency());
        slots = new Semaphore(settings.concurrency());
        meters.gauge("delivery.notification.active", active, Set::size);
    }

    @Scheduled(fixedDelayString = "${notification.poll-ms:100}", scheduler = "notificationTaskScheduler")
    public synchronized void tick() {
        for (int i = 0; i < customers.size(); i++) {
            if (!slots.tryAcquire()) return;
            long tenant = customers.get(cursor++ % customers.size());
            if (cursor >= customers.size()) cursor = 0;
            if (!active.add(tenant)) { slots.release(); continue; }
            try {
                workers.submit(() -> {
                    try { service.deliver(tenant); }
                    catch (RuntimeException failure) {
                        meters.counter("delivery.notification.events", "outcome", "work_failed").increment();
                        log.warn("Customer notification work retained: tenantId={} failure={}", tenant, failure.getClass().getSimpleName());
                    } finally { active.remove(tenant); slots.release(); }
                });
            } catch (RejectedExecutionException closing) { active.remove(tenant); slots.release(); }
        }
    }

    @Override public void close() {
        workers.shutdown();
        try { if (!workers.awaitTermination(10, TimeUnit.SECONDS)) workers.shutdownNow(); }
        catch (InterruptedException e) { workers.shutdownNow(); Thread.currentThread().interrupt(); }
    }
}

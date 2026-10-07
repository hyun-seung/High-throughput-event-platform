package messaging.webhook.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(prefix = "messaging.webhook.sender", name = "enabled", havingValue = "true")
public class WebhookDeliveryScheduler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WebhookDeliveryScheduler.class);
    private final ExecutorService workers;
    private final Semaphore slots;
    private final Set<Long> active = ConcurrentHashMap.newKeySet();
    private final List<Long> customers;
    private final WebhookDeliveryService service;
    private int cursor;

    public WebhookDeliveryScheduler(WebhookDeliveryService service, WebhookSenderProperties properties) {
        this.service = service;
        customers = properties.customers().keySet().stream().sorted().toList();
        workers = Executors.newFixedThreadPool(properties.concurrency());
        slots = new Semaphore(properties.concurrency());
    }

    @Scheduled(fixedDelayString = "${messaging.webhook.sender.poll-ms:100}")
    public synchronized void tick() {
        if (customers.isEmpty()) return;
        for (int i = 0; i < customers.size(); i++) {
            if (!slots.tryAcquire()) return;
            long customer = customers.get(cursor++ % customers.size());
            if (cursor >= customers.size()) cursor = 0;
            if (!active.add(customer)) { slots.release(); continue; }
            try {
                workers.submit(() -> {
                    try { service.deliver(customer); }
                    catch (RuntimeException error) {
                        log.warn("Customer webhook work retained: clientId={} failure={}", customer, error.getClass().getSimpleName());
                    } finally {
                        active.remove(customer);
                        slots.release();
                    }
                });
            } catch (RejectedExecutionException closing) {
                active.remove(customer);
                slots.release();
            }
        }
    }

    @Override public void close() {
        workers.shutdown();
        try { if (!workers.awaitTermination(10, TimeUnit.SECONDS)) workers.shutdownNow(); }
        catch (InterruptedException error) { workers.shutdownNow(); Thread.currentThread().interrupt(); }
    }
}

package messaging.webhook.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(prefix = "messaging.webhook.sender", name = "enabled", havingValue = "true")
public class WebhookDeliveryScheduler implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WebhookDeliveryScheduler.class);
    private final ExecutorService workers;
    private final int concurrency;
    // All queue/active state is guarded by this scheduler's monitor.
    private final Set<Long> active = new HashSet<>();
    private final Set<Long> queued = new HashSet<>();
    private final ArrayDeque<Long> pending = new ArrayDeque<>();
    private final List<Long> customers;
    private final WebhookDeliveryService service;
    private boolean closing;

    public WebhookDeliveryScheduler(WebhookDeliveryService service, WebhookSenderProperties properties) {
        this.service = service;
        customers = properties.customers().keySet().stream().sorted().toList();
        concurrency = properties.concurrency();
        workers = Executors.newFixedThreadPool(concurrency, Thread.ofPlatform().name("customer-webhook-", 0).factory());
        log.info("Customer webhook scheduler started with {} configured destinations", customers.size());
    }

    @Scheduled(fixedDelayString = "${messaging.webhook.sender.poll-ms:100}")
    public synchronized void tick() {
        if (closing) return;
        for (long customer : customers) enqueue(customer);
        dispatch();
    }

    private void enqueue(long customer) {
        if (!active.contains(customer) && queued.add(customer)) pending.addLast(customer);
    }

    private void dispatch() {
        while (!closing && !pending.isEmpty() && active.size() < concurrency) {
            long customer = pending.removeFirst();
            queued.remove(customer);
            active.add(customer);
            try {
                workers.submit(() -> {
                    boolean more = false;
                    try { more = service.deliver(customer); }
                    catch (RuntimeException error) {
                        log.warn("Customer webhook work retained: clientId={} failure={}", customer, error.getClass().getSimpleName());
                    } finally {
                        finished(customer, more);
                    }
                });
            } catch (RejectedExecutionException closing) {
                active.remove(customer);
            }
        }
    }

    private synchronized void finished(long customer, boolean more) {
        active.remove(customer);
        if (more && !closing) enqueue(customer);
        dispatch();
    }

    @Override public void close() {
        synchronized (this) {
            closing = true;
            pending.clear();
            queued.clear();
        }
        workers.shutdown();
        try { if (!workers.awaitTermination(10, TimeUnit.SECONDS)) workers.shutdownNow(); }
        catch (InterruptedException error) { workers.shutdownNow(); Thread.currentThread().interrupt(); }
    }
}

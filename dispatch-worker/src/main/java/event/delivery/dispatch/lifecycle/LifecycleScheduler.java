package event.delivery.dispatch.lifecycle;

import event.common.lifecycle.LifecycleIndex;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;

/** GSI discovers candidates; strong reads and conditional writes decide work. No Scan or TTL dependency. */
public class LifecycleScheduler {
    private static final Logger log = LoggerFactory.getLogger(LifecycleScheduler.class);
    private final LifecycleRepository repository;
    private final LifecycleService service;
    private final Clock clock;
    private final MeterRegistry metrics;
    private final int pageSize;
    private final Semaphore capacity;
    private final ExecutorService workers;
    private final Set<String> active = ConcurrentHashMap.newKeySet();
    private event.common.redis.DeliveryCache cache = event.common.redis.DeliveryCache.UNAVAILABLE;
    private java.time.Instant lastSweep;
    private long recoveryIntervalMs;
    public void cache(event.common.redis.DeliveryCache cache, long recoveryIntervalMs) {
        if (recoveryIntervalMs < 1000) throw new IllegalArgumentException("Recovery interval must be >= 1000ms");
        this.cache = cache; this.recoveryIntervalMs = recoveryIntervalMs;
    }
    private int nextShard;
    private final Map<Integer, Map<String, AttributeValue>> cursors = new HashMap<>();
    public LifecycleScheduler(LifecycleRepository repository, LifecycleService service, Clock clock, MeterRegistry metrics,
                              int pageSize, int concurrency) {
        if (pageSize < 1 || pageSize > 1000 || concurrency < 1 || concurrency > 64) throw new IllegalArgumentException("Invalid lifecycle limits");
        this.repository = repository; this.service = service; this.clock = clock; this.metrics = metrics; this.pageSize = pageSize;
        this.capacity = new Semaphore(concurrency);
        workers = Executors.newFixedThreadPool(concurrency, Thread.ofPlatform().daemon().name("lifecycle-", 0).factory());
        metrics.gauge("delivery.lifecycle.active", active, Set::size);
    }

    @Scheduled(fixedDelayString = "${dispatch.lifecycle.poll-ms:1000}")
    public synchronized void tick() {
        for (String execution : cache.due(clock.instant(), pageSize)) {
            if (!active.add(execution)) continue;
            if (!capacity.tryAcquire()) { active.remove(execution); break; }
            try {
                workers.submit(() -> {
                    try { service.reconcile(execution); }
                    catch (RuntimeException failure) { metrics.counter("delivery.lifecycle.events", "outcome", "work_failed").increment(); }
                    finally { active.remove(execution); capacity.release(); }
                });
            } catch (RejectedExecutionException closing) { active.remove(execution); capacity.release(); return; }
        }
        if (lastSweep != null && clock.instant().isBefore(lastSweep.plusMillis(recoveryIntervalMs))) return;
        lastSweep = clock.instant();
        int start = nextShard;
        nextShard = (nextShard + 1) % LifecycleIndex.SHARDS;
        for (int offset = 0; offset < LifecycleIndex.SHARDS; offset++) {
            int shard = (start + offset) % LifecycleIndex.SHARDS;
            try {
                var page = repository.due(shard, clock.instant(), pageSize, cursors.getOrDefault(shard, Map.of()));
                boolean scheduledAll = true;
                Map<String, AttributeValue> lastVisited = cursors.getOrDefault(shard, Map.of());
                for (var key : page.items()) {
                    String delivery = key.get("pk").s().substring("DELIVERY#".length());
                    if (!active.add(delivery)) { lastVisited = key; continue; }
                    if (!capacity.tryAcquire()) { active.remove(delivery); scheduledAll = false; break; }
                    try {
                        workers.submit(() -> {
                            try { service.reconcile(delivery); }
                            catch (RuntimeException failure) {
                                metrics.counter("delivery.lifecycle.events", "outcome", "work_failed").increment();
                                log.warn("Lifecycle work retained. deliveryId={}, failure={}", delivery, failure.getClass().getSimpleName());
                            } finally { active.remove(delivery); capacity.release(); }
                        });
                        lastVisited = key;
                    } catch (RejectedExecutionException closing) { active.remove(delivery); capacity.release(); return; }
                }
                // Continue pages, including poison records; do not always restart at an unprocessable oldest row.
                cursors.put(shard, scheduledAll ? page.lastEvaluatedKey() : lastVisited);
            } catch (RuntimeException failure) {
                metrics.counter("delivery.lifecycle.events", "outcome", "query_failed").increment();
                log.warn("Lifecycle query retained. shard={}, failure={}", shard, failure.getClass().getSimpleName());
            }
        }
    }
    @PreDestroy public void close() { workers.shutdownNow(); }
}

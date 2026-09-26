package event.delivery.dispatch.lifecycle;

import event.common.lifecycle.LifecycleIndex;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import event.common.recovery.FailureBackoff;

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
    private final FailureBackoff queries, work;
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
        this(repository, service, clock, metrics, pageSize, concurrency,
                new FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30)),
                new FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30)));
    }
    public LifecycleScheduler(LifecycleRepository repository, LifecycleService service, Clock clock, MeterRegistry metrics,
                              int pageSize, int concurrency, FailureBackoff queries, FailureBackoff work) {
        if (pageSize < 1 || pageSize > 1000 || concurrency < 1 || concurrency > 64) throw new IllegalArgumentException("Invalid lifecycle limits");
        this.repository = repository; this.service = service; this.clock = clock; this.metrics = metrics; this.pageSize = pageSize;
        this.queries = queries; this.work = work;
        this.capacity = new Semaphore(concurrency);
        workers = Executors.newFixedThreadPool(concurrency, Thread.ofPlatform().daemon().name("lifecycle-", 0).factory());
        metrics.gauge("delivery.lifecycle.active", active, Set::size);
        metrics.gauge("delivery.lifecycle.query.recovery.delay", queries, FailureBackoff::remainingSeconds);
        metrics.gauge("delivery.lifecycle.work.recovery.delay", work, FailureBackoff::remainingSeconds);
    }

    @Scheduled(fixedDelayString = "${dispatch.lifecycle.poll-ms:1000}")
    public synchronized void tick() {
        if (capacity.availablePermits() == 0 || work.blocked()) {
            metrics.counter("delivery.lifecycle.events", "outcome", "poll_deferred").increment(); return;
        }
        // Reserve the due recovery opportunity before hot Redis candidates can occupy every worker.
        if (lastSweep == null || !clock.instant().isBefore(lastSweep.plusMillis(recoveryIntervalMs))) sweep();
        if (capacity.availablePermits() == 0 || work.blocked()) return;
        for (String execution : cache.due(clock.instant(), pageSize)) {
            if (!submit(execution)) break;
        }
    }
    private void sweep() {
        var queryTicket = queries.acquire();
        if (queryTicket == null) { metrics.counter("delivery.lifecycle.events", "outcome", "query_deferred").increment(); return; }
        boolean queried = false;
        int start = nextShard;
        nextShard = (nextShard + 1) % LifecycleRepository.RECOVERY_SHARDS;
        for (int offset = 0; offset < LifecycleRepository.RECOVERY_SHARDS; offset++) {
            if (capacity.availablePermits() == 0 || work.blocked()) break;
            int shard = (start + offset) % LifecycleRepository.RECOVERY_SHARDS;
            try {
                if (!queried) lastSweep = clock.instant();
                queried = true;
                var page = repository.due(shard, clock.instant(), pageSize, cursors.getOrDefault(shard, Map.of()));
                boolean scheduledAll = true;
                Map<String, AttributeValue> lastVisited = cursors.getOrDefault(shard, Map.of());
                for (var key : page.items()) {
                    String delivery = key.get("pk").s().substring("DELIVERY#".length());
                    if (!submit(delivery)) { scheduledAll = false; break; }
                    lastVisited = key;
                }
                // Continue pages, including poison records; do not always restart at an unprocessable oldest row.
                cursors.put(shard, scheduledAll ? page.lastEvaluatedKey() : lastVisited);
            } catch (RuntimeException failure) {
                metrics.counter("delivery.lifecycle.events", "outcome", "query_failed").increment();
                log.warn("Lifecycle query retained. shard={}, failure={}", shard, failure.getClass().getSimpleName());
                queries.failed(queryTicket);
                return; // Do not issue the same failing query against every remaining shard.
            }
        }
        if (queried) queries.succeeded(queryTicket); else queries.abandon(queryTicket);
    }
    private boolean submit(String delivery) {
        if (!active.add(delivery)) return true;
        if (!capacity.tryAcquire()) { active.remove(delivery); return false; }
        var ticket = work.acquire();
        if (ticket == null) { active.remove(delivery); capacity.release(); return false; }
        try {
            workers.submit(() -> {
                try { service.reconcile(delivery); work.succeeded(ticket); }
                catch (RuntimeException failure) {
                    if (backendFailure(failure)) work.failed(ticket); else work.abandon(ticket);
                    metrics.counter("delivery.lifecycle.events", "outcome", "work_failed").increment();
                    log.warn("Lifecycle work retained. deliveryId={}, failure={}", delivery, failure.getClass().getSimpleName());
                } finally { active.remove(delivery); capacity.release(); }
            });
            return true;
        } catch (RejectedExecutionException closing) {
            work.abandon(ticket); active.remove(delivery); capacity.release(); return false;
        }
    }
    private static boolean backendFailure(Throwable failure) {
        for (int depth = 0; failure != null && depth < 12; depth++, failure = failure.getCause()) {
            if (failure instanceof software.amazon.awssdk.core.exception.SdkClientException
                    || failure instanceof org.apache.kafka.common.KafkaException || failure instanceof TimeoutException) return true;
            if (failure instanceof software.amazon.awssdk.services.dynamodb.model.DynamoDbException ddb
                    && (ddb.statusCode() >= 500 || ddb.statusCode() == 429
                    || ddb instanceof software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException
                    || ddb instanceof software.amazon.awssdk.services.dynamodb.model.RequestLimitExceededException)) return true;
        }
        return false;
    }
    @PreDestroy public void close() { workers.shutdownNow(); }
}

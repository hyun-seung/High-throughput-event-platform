package event.common.redis;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.time.*;
import java.util.*;
import java.util.function.Supplier;
import event.common.recovery.FailureBackoff;

/** Optional acceleration only. A cache failure never grants a DynamoDB claim. */
public class DeliveryCache {
    public static final DeliveryCache UNAVAILABLE = new DeliveryCache(null, null, Duration.ofHours(24));
    private final StringRedisTemplate redis;
    private final MeterRegistry metrics;
    private final Duration retention;
    private final FailureBackoff backoff;
    public DeliveryCache(StringRedisTemplate redis, MeterRegistry metrics, Duration retention) {
        this(redis, metrics, retention, new FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30)));
    }
    public DeliveryCache(StringRedisTemplate redis, MeterRegistry metrics, Duration retention, FailureBackoff backoff) {
        if (retention.isNegative() || retention.isZero()) throw new IllegalArgumentException("Positive completion retention required");
        this.redis = redis; this.metrics = metrics; this.retention = retention; this.backoff = backoff;
    }
    public String completed(String requestKey) {
        return safely("read", () -> redis.opsForValue().get("delivery:completed:" + requestKey), null);
    }
    public void complete(String requestKey, String fingerprint, Instant finalizedAt) {
        // An absolute business timestamp prevents retries from extending the retention window.
        var remaining = Duration.between(Instant.now(), finalizedAt.plus(retention));
        if (remaining.isNegative() || remaining.isZero()) return;
        safely("complete", () -> { redis.opsForValue().set("delivery:completed:" + requestKey, fingerprint, remaining); return true; }, false);
    }
    public void schedule(String executionId, Instant due) {
        safely("schedule", () -> redis.opsForZSet().add("delivery:due", executionId, due.toEpochMilli()), false);
    }
    public void removeSchedule(String executionId) {
        safely("remove", () -> redis.opsForZSet().remove("delivery:due", executionId), 0L);
    }
    public Set<String> due(Instant now, int limit) {
        return safely("due", () -> {
            var values = redis.opsForZSet().rangeByScore("delivery:due", 0, now.toEpochMilli(), 0, limit);
            return values == null ? Set.of() : values;
        }, Set.of());
    }
    private <T> T safely(String operation, Supplier<T> action, T fallback) {
        if (redis == null) return fallback;
        var ticket = backoff.acquire();
        if (ticket == null) {
            if (metrics != null) metrics.counter("delivery.redis.calls.skipped", "operation", operation).increment();
            return fallback;
        }
        try { T result = action.get(); backoff.succeeded(ticket); return result; }
        catch (RuntimeException unavailable) {
            backoff.failed(ticket);
            if (metrics != null) metrics.counter("delivery.cache.errors", "operation", operation).increment();
            return fallback;
        }
    }
}

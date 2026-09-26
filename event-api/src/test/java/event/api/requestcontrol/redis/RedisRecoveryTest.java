package event.api.requestcontrol.redis;

import event.api.requestcontrol.result.RequestLimitStatus;
import event.common.recovery.FailureBackoff;
import event.common.redis.DeliveryCache;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisRecoveryTest {
    @Test void outageSkipsRepeatedCallsAndRecoveryRestoresTpsDecision() {
        var redis = mock(StringRedisTemplate.class); RedisScript<List> script = mock(RedisScript.class); var now = new AtomicLong();
        var gate = new FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(4), now::get, () -> 1);
        when(redis.execute(eq(script), anyList())).thenThrow(new RedisConnectionFailureException("down")).thenReturn(List.of(2L, 0L, 1L, 100L));
        var metrics = new SimpleMeterRegistry();
        try {
            var limiter = new RedisRequestLimiter(redis, script, gate, metrics);
            for (int i = 0; i < 1000; i++) assertEquals(RequestLimitStatus.REDIS_UNAVAILABLE_BYPASS, limiter.tryAcquire(42L).status());
            verify(redis, times(1)).execute(eq(script), anyList());
            assertEquals(999, metrics.get("delivery.redis.calls.skipped").tag("operation", "request_limit").counter().count());
            now.set(1_000_000_000); assertEquals(RequestLimitStatus.TPS_LIMIT_EXCEEDED, limiter.tryAcquire(42L).status());
            assertEquals(RequestLimitStatus.TPS_LIMIT_EXCEEDED, limiter.tryAcquire(42L).status()); assertFalse(gate.blocked());
        } finally { metrics.close(); }
    }
    @Test void invalidPolicyResponseDoesNotBecomeAnUnlimitedAllowance() {
        var redis = mock(StringRedisTemplate.class); RedisScript<List> script = mock(RedisScript.class);
        when(redis.execute(eq(script), anyList())).thenReturn(List.of());
        var gate = new FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(4));
        var metrics = new SimpleMeterRegistry();
        try {
            var limiter = new RedisRequestLimiter(redis, script, gate, metrics);
            assertThrows(IllegalStateException.class, () -> limiter.tryAcquire(42L)); assertFalse(gate.blocked());
        } finally { metrics.close(); }
    }
    @Test void cacheAndRequestLimiterShareOutageButCacheRecoveryDoesNotBypassPolicy() {
        var redis = mock(StringRedisTemplate.class); RedisScript<List> script = mock(RedisScript.class); var now = new AtomicLong();
        var gate = new FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(4), now::get, () -> 1);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenThrow(new RedisConnectionFailureException("down")).thenReturn(values);
        when(values.get("delivery:completed:request")).thenReturn("fingerprint");
        when(redis.execute(eq(script), anyList())).thenReturn(List.of(3L, 1L, 100L, 100L));
        var metrics = new SimpleMeterRegistry();
        try {
            var cache = new DeliveryCache(redis, metrics, Duration.ofHours(24), gate);
            var limiter = new RedisRequestLimiter(redis, script, gate, metrics);
            assertNull(cache.completed("request")); assertTrue(limiter.tryAcquire(42L).isAllowed());
            verify(redis, never()).execute(eq(script), anyList());
            now.set(1_000_000_000); assertEquals("fingerprint", cache.completed("request"));
            assertEquals(RequestLimitStatus.MONTHLY_QUOTA_EXCEEDED, limiter.tryAcquire(42L).status());
        } finally { metrics.close(); }
    }
}

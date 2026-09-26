package event.api.requestcontrol.redis;

import event.common.recovery.FailureBackoff;
import event.common.redis.DeliveryCache;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.time.*;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@EnabledIfEnvironmentVariable(named = "REDIS_TEST_PORT", matches = ".+")
class RedisRecoveryIntegrationTest {
    @Test void skippedScheduleIsNotInventedOnRecoveryAndNewCallsUseActualRedis() {
        var factory = new LettuceConnectionFactory("localhost", Integer.parseInt(System.getenv("REDIS_TEST_PORT")));
        factory.afterPropertiesSet(); factory.start(); var redis = new StringRedisTemplate(factory);
        String id = UUID.randomUUID().toString(); var now = new AtomicLong();
        var metrics = new SimpleMeterRegistry();
        try {
            var fault = mock(StringRedisTemplate.class, org.mockito.AdditionalAnswers.delegatesTo(redis));
            when(fault.opsForZSet()).thenThrow(new RedisConnectionFailureException("injected connection loss")).thenReturn(redis.opsForZSet());
            var cache = new DeliveryCache(fault, metrics, Duration.ofHours(24),
                    new FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(4), now::get, () -> 1));
            cache.schedule(id, Instant.EPOCH);
            for (int i = 0; i < 100; i++) assertTrue(cache.due(Instant.now(), 10).isEmpty());
            verify(fault, times(1)).opsForZSet(); assertNull(redis.opsForZSet().score("delivery:due", id));
            now.set(1_000_000_000); cache.due(Instant.now(), 10); // A real Redis read closes the gate.
            cache.schedule(id, Instant.EPOCH); assertEquals(0d, redis.opsForZSet().score("delivery:due", id));
            cache.complete(id, "fingerprint", Instant.now()); assertEquals("fingerprint", cache.completed(id));
            assertTrue(redis.getExpire("delivery:completed:" + id) > 0);
            cache.removeSchedule(id); assertNull(redis.opsForZSet().score("delivery:due", id));
        } finally {
            metrics.close();
            redis.opsForZSet().remove("delivery:due", id); redis.delete("delivery:completed:" + id); factory.destroy();
        }
    }
}

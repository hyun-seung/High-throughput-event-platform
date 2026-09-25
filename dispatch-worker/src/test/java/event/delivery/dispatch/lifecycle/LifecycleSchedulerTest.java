package event.delivery.dispatch.lifecycle;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LifecycleSchedulerTest {
    @Test void tenMinuteRecoveryKeepsRedisPollingAndRecoversMissingSchedule() throws Exception {
        var repository = mock(LifecycleRepository.class);
        var service = mock(LifecycleService.class);
        var cache = mock(event.common.redis.DeliveryCache.class);
        var clock = mock(Clock.class);
        var start = Instant.parse("2026-09-25T00:00:00Z");
        var now = new AtomicReference<>(start);
        when(clock.instant()).thenAnswer(call -> now.get());
        when(cache.due(any(), anyInt())).thenReturn(Set.of());
        when(cache.due(start.plusSeconds(1), 10)).thenReturn(Set.of("redis-present"));
        // Only the durable index knows about the missing Redis schedule.
        when(repository.due(anyInt(), any(), anyInt(), anyMap())).thenAnswer(call ->
                call.getArgument(0, Integer.class) == 0 && !now.get().isBefore(start.plusSeconds(600))
                        ? QueryResponse.builder().items(key("redis-missing")).build()
                        : QueryResponse.builder().build());
        var redisProcessed = new CountDownLatch(1);
        var missingRecovered = new CountDownLatch(1);
        doAnswer(call -> { redisProcessed.countDown(); return null; }).when(service).reconcile("redis-present");
        doAnswer(call -> { missingRecovered.countDown(); return null; }).when(service).reconcile("redis-missing");
        var metrics = new SimpleMeterRegistry();
        try {
            var scheduler = new LifecycleScheduler(repository, service, clock, metrics, 10, 2);
            scheduler.cache(cache, 600_000);
            try {
                scheduler.tick(); // Immediate startup recovery.
                verify(repository, times(LifecycleRepository.RECOVERY_SHARDS)).due(anyInt(), any(), anyInt(), anyMap());
                now.set(start.plusSeconds(1));
                scheduler.tick();
                assertTrue(redisProcessed.await(2, TimeUnit.SECONDS));
                now.set(start.plusMillis(599_999));
                scheduler.tick();
                verify(repository, times(LifecycleRepository.RECOVERY_SHARDS)).due(anyInt(), any(), anyInt(), anyMap());
                verify(service, never()).reconcile("redis-missing");
                now.set(start.plusSeconds(600));
                scheduler.tick();
                assertTrue(missingRecovered.await(2, TimeUnit.SECONDS));
                verify(repository, times(2 * LifecycleRepository.RECOVERY_SHARDS)).due(anyInt(), any(), anyInt(), anyMap());
                verify(cache, times(4)).due(any(), eq(10));
            } finally { scheduler.close(); }
        } finally { metrics.close(); }
    }

    @Test void failedOldestCandidateDoesNotStarveLaterPagesWithOneWorker() throws Exception {
        var repository = mock(LifecycleRepository.class);
        var service = mock(LifecycleService.class);
        var first = key("old");
        var second = key("next");
        when(repository.due(anyInt(), any(), anyInt(), anyMap())).thenAnswer(call -> {
            if (call.getArgument(0, Integer.class) != 0) return QueryResponse.builder().build();
            Map<String, AttributeValue> cursor = call.getArgument(3);
            if (cursor.isEmpty()) return QueryResponse.builder().items(first).lastEvaluatedKey(first).build();
            return QueryResponse.builder().items(second).build();
        });
        var failed = new CountDownLatch(1); var next = new CountDownLatch(1);
        doAnswer(call -> { failed.countDown(); throw new IllegalStateException("poison input"); }).when(service).reconcile("old");
        doAnswer(call -> { next.countDown(); return null; }).when(service).reconcile("next");
        var metrics = new SimpleMeterRegistry();
        try {
            var scheduler = new LifecycleScheduler(repository, service, Clock.systemUTC(), metrics, 1, 1);
            try {
                scheduler.tick(); assertTrue(failed.await(2, TimeUnit.SECONDS));
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (next.getCount() != 0 && System.nanoTime() < until) { scheduler.tick(); Thread.sleep(10); }
                assertEquals(0, next.getCount());
                assertTrue(metrics.find("delivery.lifecycle.events").tag("outcome", "work_failed").counter().count() >= 1);
            } finally { scheduler.close(); }
        } finally { metrics.close(); }
    }
    private Map<String, AttributeValue> key(String id) {
        return Map.of("pk", AttributeValue.fromS("DELIVERY#" + id), "sk", AttributeValue.fromS("META"),
                "lifecycle_bucket", AttributeValue.fromS("lifecycle-v1-0"), "lifecycle_due", AttributeValue.fromN("0"));
    }
}

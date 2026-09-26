package event.delivery.dispatch.lifecycle;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import java.time.Duration;
import event.common.recovery.FailureBackoff;
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
    FailureBackoff gate(AtomicLong now) { return new FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(4), now::get, () -> 1); }
    void idle(SimpleMeterRegistry metrics) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (metrics.get("delivery.lifecycle.active").gauge().value() != 0 && System.nanoTime() < end) Thread.sleep(1);
        assertEquals(0, metrics.get("delivery.lifecycle.active").gauge().value());
    }

    @Test void queryOutageStopsAtFirstFailureAndNeverShortensTenMinuteRecoveryInterval() {
        var repository = mock(LifecycleRepository.class); var service = mock(LifecycleService.class);
        when(repository.due(anyInt(), any(), anyInt(), anyMap())).thenThrow(new IllegalStateException("down"));
        var wall = new AtomicReference<>(Instant.parse("2026-09-25T00:00:00Z")); var clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(call -> wall.get()); var nanos = new AtomicLong();
        var metrics = new SimpleMeterRegistry();
        try {
            var scheduler = new LifecycleScheduler(repository, service, clock, metrics, 10, 1, gate(nanos), gate(nanos));
            scheduler.cache(event.common.redis.DeliveryCache.UNAVAILABLE, 600_000);
            try {
                scheduler.tick(); verify(repository, times(1)).due(anyInt(), any(), anyInt(), anyMap());
                nanos.set(599_000_000_000L); wall.set(wall.get().plusSeconds(599));
                for (int i = 0; i < 100; i++) scheduler.tick();
                verify(repository, times(1)).due(anyInt(), any(), anyInt(), anyMap());
                nanos.addAndGet(1_000_000_000); wall.set(wall.get().plusSeconds(1)); scheduler.tick();
                verify(repository, times(2)).due(anyInt(), any(), anyInt(), anyMap());
            } finally { scheduler.close(); }
        } finally { metrics.close(); }
    }

    @Test void failedQueryPreservesPageCursorForRecovery() {
        var repository = mock(LifecycleRepository.class); var service = mock(LifecycleService.class); var cursor = key("last");
        var calls = new java.util.concurrent.atomic.AtomicInteger(); var nanos = new AtomicLong();
        when(repository.due(anyInt(), any(), anyInt(), anyMap())).thenAnswer(call -> {
            if (call.getArgument(0, Integer.class) != 0) return QueryResponse.builder().build();
            int attempt = calls.incrementAndGet();
            if (attempt == 1) return QueryResponse.builder().lastEvaluatedKey(cursor).build();
            assertEquals(cursor, call.getArgument(3));
            if (attempt == 2) throw new IllegalStateException("query unavailable");
            return QueryResponse.builder().build();
        });
        var metrics = new SimpleMeterRegistry();
        try {
            var scheduler = new LifecycleScheduler(repository, service, Clock.systemUTC(), metrics, 10, 1, gate(nanos), gate(nanos));
            try {
                scheduler.tick(); scheduler.tick(); assertEquals(2, calls.get());
                scheduler.tick(); assertEquals(2, calls.get());
                nanos.set(1_000_000_000); scheduler.tick(); assertEquals(3, calls.get());
            } finally { scheduler.close(); }
        } finally { metrics.close(); }
    }

    @Test void backendWorkFailurePausesPollingAndAdmitsOneRecoveryProbe() throws Exception {
        var repository = mock(LifecycleRepository.class); var service = mock(LifecycleService.class);
        var cache = mock(event.common.redis.DeliveryCache.class); var nanos = new AtomicLong();
        when(repository.due(anyInt(), any(), anyInt(), anyMap())).thenReturn(QueryResponse.builder().build());
        when(cache.due(any(), anyInt())).thenReturn(Set.of("retained"));
        var probeEntered = new CountDownLatch(1); var release = new CountDownLatch(1); var calls = new java.util.concurrent.atomic.AtomicInteger();
        doAnswer(call -> {
            if (calls.incrementAndGet() == 1) throw software.amazon.awssdk.core.exception.SdkClientException.create("DDB down");
            probeEntered.countDown(); assertTrue(release.await(3, TimeUnit.SECONDS)); return null;
        }).when(service).reconcile("retained");
        var metrics = new SimpleMeterRegistry();
        try {
            var scheduler = new LifecycleScheduler(repository, service, Clock.systemUTC(), metrics, 10, 2, gate(nanos), gate(nanos));
            scheduler.cache(cache, 600_000);
            try {
                scheduler.tick(); idle(metrics);
                for (int i = 0; i < 100; i++) scheduler.tick();
                assertEquals(1, calls.get()); verify(cache, times(1)).due(any(), anyInt());
                nanos.set(1_000_000_000); scheduler.tick(); assertTrue(probeEntered.await(3, TimeUnit.SECONDS));
                for (int i = 0; i < 100; i++) scheduler.tick();
                assertEquals(2, calls.get()); verify(cache, times(2)).due(any(), anyInt());
                release.countDown(); idle(metrics); scheduler.tick(); idle(metrics); assertEquals(3, calls.get());
            } finally { release.countDown(); scheduler.close(); }
        } finally { metrics.close(); }
    }

    @Test void saturatedWorkerDoesNotKeepFetchingCandidates() throws Exception {
        var repository = mock(LifecycleRepository.class); var service = mock(LifecycleService.class);
        when(repository.due(anyInt(), any(), anyInt(), anyMap())).thenReturn(QueryResponse.builder().build());
        var cache = mock(event.common.redis.DeliveryCache.class); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        when(cache.due(any(), anyInt())).thenReturn(new LinkedHashSet<>(List.of("one", "two")));
        doAnswer(call -> { entered.countDown(); assertTrue(release.await(3, TimeUnit.SECONDS)); return null; }).when(service).reconcile("one");
        var metrics = new SimpleMeterRegistry();
        try {
            var scheduler = new LifecycleScheduler(repository, service, Clock.systemUTC(), metrics, 10, 1);
            scheduler.cache(cache, 600_000);
            try {
                scheduler.tick(); assertTrue(entered.await(3, TimeUnit.SECONDS));
                clearInvocations(repository);
                for (int i = 0; i < 100; i++) scheduler.tick();
                verify(cache, times(1)).due(any(), anyInt()); verifyNoInteractions(repository); verify(service, never()).reconcile("two");
            } finally { release.countDown(); idle(metrics); scheduler.close(); }
        } finally { metrics.close(); }
    }

    @Test void tenMinuteSweepGetsCapacityBeforeContinuouslyDueRedisCandidates() throws Exception {
        var repository = mock(LifecycleRepository.class); var service = mock(LifecycleService.class);
        var cache = mock(event.common.redis.DeliveryCache.class); var clock = mock(Clock.class);
        var start = Instant.parse("2026-09-25T00:00:00Z"); var now = new AtomicReference<>(start);
        when(clock.instant()).thenAnswer(call -> now.get()); when(cache.due(any(), anyInt())).thenReturn(Set.of("redis-busy"));
        when(repository.due(anyInt(), any(), anyInt(), anyMap())).thenAnswer(call ->
                call.getArgument(0, Integer.class) == 0 && now.get().equals(start.plusSeconds(600))
                        ? QueryResponse.builder().items(key("redis-missing")).build() : QueryResponse.builder().build());
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        doAnswer(call -> { entered.countDown(); assertTrue(release.await(3, TimeUnit.SECONDS)); return null; }).when(service).reconcile("redis-missing");
        var metrics = new SimpleMeterRegistry();
        var scheduler = new LifecycleScheduler(repository, service, clock, metrics, 10, 1); scheduler.cache(cache, 600_000);
        try {
            scheduler.tick(); idle(metrics); verify(service).reconcile("redis-busy");
            now.set(start.plusSeconds(600)); scheduler.tick(); assertTrue(entered.await(3, TimeUnit.SECONDS));
            verify(cache, times(1)).due(any(), anyInt()); verify(service).reconcile("redis-missing");
        } finally { release.countDown(); idle(metrics); scheduler.close(); metrics.close(); }
    }

    private Map<String, AttributeValue> key(String id) {
        return Map.of("pk", AttributeValue.fromS("DELIVERY#" + id), "sk", AttributeValue.fromS("META"),
                "lifecycle_bucket", AttributeValue.fromS("lifecycle-v1-0"), "lifecycle_due", AttributeValue.fromN("0"));
    }
}

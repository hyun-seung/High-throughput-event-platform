package event.delivery.dispatch.lifecycle;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LifecycleSchedulerTest {
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

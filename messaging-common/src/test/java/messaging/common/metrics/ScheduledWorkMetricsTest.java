package messaging.common.metrics;

import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static messaging.common.metrics.ScheduledWorkMetrics.Phase.*;
import static org.junit.jupiter.api.Assertions.*;

class ScheduledWorkMetricsTest {
    private final MockClock clock = new MockClock();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);
    private final ScheduledWorkMetrics metrics = new ScheduledWorkMetrics(registry,
            ScheduledWorkMetrics.Worker.RESULT_INBOX);

    @Test
    void separatesWorkFromIdleAndRecordsExceptionalExit() {
        assertThrows(IllegalStateException.class, () -> {
            try (var poll = metrics.start(POLL)) {
                clock.add(Duration.ofSeconds(2));
                try (var item = metrics.start(ITEM)) {
                    clock.add(Duration.ofSeconds(3));
                    throw new IllegalStateException("test failure");
                }
            }
        });
        clock.add(Duration.ofSeconds(5));
        var next = metrics.start(POLL);
        clock.add(Duration.ofSeconds(1));
        next.close();
        next.close();

        var poll = registry.get("messaging.scheduled.work.duration").tag("phase", "poll").timer();
        assertEquals(2, poll.count());
        assertEquals(6, poll.totalTime(TimeUnit.SECONDS));
        assertEquals(3, registry.get("messaging.scheduled.work.duration").tag("phase", "item").timer()
                .totalTime(TimeUnit.SECONDS));
        assertEquals(5, registry.get("messaging.scheduled.work.idle").timer().totalTime(TimeUnit.SECONDS));
        assertEquals(1, registry.get("messaging.scheduled.work.idle").timer().count());
    }

    @Test
    void distinguishesSharedIndexCandidatesAndClampsFutureDueTime() {
        metrics.page(100, 40);
        metrics.due(1000, 4000);
        metrics.due(5000, 4000);
        assertEquals(100, registry.get("messaging.scheduled.work.page.items").tag("kind", "candidate").summary().totalAmount());
        assertEquals(40, registry.get("messaging.scheduled.work.page.items").tag("kind", "eligible").summary().totalAmount());
        assertEquals(2, registry.get("messaging.scheduled.work.due.age").timer().count());
        assertEquals(3, registry.get("messaging.scheduled.work.due.age").timer().totalTime(TimeUnit.SECONDS));
        registry.getMeters().forEach(meter -> meter.getId().getTags().forEach(tag ->
                assertTrue(java.util.Set.of("worker", "phase", "kind", "le").contains(tag.getKey()), tag.toString())));
    }
}

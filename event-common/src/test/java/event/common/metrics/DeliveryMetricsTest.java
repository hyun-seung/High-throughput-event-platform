package event.common.metrics;

import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static event.common.metrics.DeliveryMetrics.Stage.API_PUBLISH;
import static org.junit.jupiter.api.Assertions.*;

class DeliveryMetricsTest {
    private final MockClock clock = new MockClock();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);
    private final DeliveryMetrics metrics = new DeliveryMetrics(registry);

    @Test
    void asynchronousTimingIncludesAckWaitAndReleasesActiveCount() {
        var ack = new CompletableFuture<String>();
        var measured = metrics.measureAsync(API_PUBLISH, () -> ack);
        assertEquals(1, active());
        assertEquals(0, timer("success").count());
        clock.add(Duration.ofSeconds(2));
        ack.complete("accepted");
        assertEquals("accepted", measured.join());
        assertEquals(0, active());
        assertEquals(1, timer("success").count());
        assertEquals(2, timer("success").totalTime(TimeUnit.SECONDS));
    }

    @Test
    void asynchronousFailurePreservesCauseAndDoesNotCountSuccess() {
        var ack = new CompletableFuture<String>();
        var measured = metrics.measureAsync(API_PUBLISH, () -> ack);
        var error = new IllegalStateException("ack unavailable");
        ack.completeExceptionally(error);
        assertSame(error, assertThrows(CompletionException.class, measured::join).getCause());
        assertEquals(1, timer("failure").count());
        assertEquals(0, timer("success").count());
        assertEquals(0, active());
    }

    @Test
    void synchronousFailureAlsoReleasesActiveCountAndKeepsOriginalException() {
        var error = new IllegalArgumentException("serialization failed");
        assertSame(error, assertThrows(IllegalArgumentException.class,
                () -> metrics.measureAsync(API_PUBLISH, () -> { throw error; })));
        assertSame(error, assertThrows(IllegalArgumentException.class,
                () -> metrics.measure(API_PUBLISH, (Runnable) () -> { throw error; })));
        assertEquals(2, timer("failure").count());
        assertEquals(0, active());
    }

    @Test
    void reversedWallClockIsReportedWithoutInventingZeroLatency() {
        var ingress = Instant.parse("2026-09-24T00:00:00Z");
        metrics.accepted(ingress, ingress.plusSeconds(3));
        metrics.accepted(ingress, ingress.minusSeconds(1));
        assertEquals(1, registry.get("delivery.acceptance.latency").timer().count());
        assertEquals(3, registry.get("delivery.acceptance.latency").timer().totalTime(TimeUnit.SECONDS));
        assertEquals(1, registry.get("delivery.acceptance.invalid.timestamp").counter().count());
        assertTrue(registry.getMeters().stream().flatMap(m -> m.getId().getTags().stream())
                .allMatch(tag -> tag.getKey().equals("stage") || tag.getKey().equals("result") || tag.getKey().equals("outcome")));
    }

    private Timer timer(String result) {
        return registry.get("delivery.stage.duration").tags("stage", "api_publish", "result", result).timer();
    }
    private double active() { return registry.get("delivery.stage.active").tag("stage", "api_publish").gauge().value(); }
}

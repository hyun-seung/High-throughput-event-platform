package messaging.common.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DynamoDbTransportMetricsTest {
    @Test
    void nestedAttemptsRetainAllTimingsAndPoolStateIsUnknownBeforeItsFirstObservation() {
        var registry = new SimpleMeterRegistry();
        try {
            var publisher = new DynamoDbTransportMetrics(registry);
            assertTrue(Double.isNaN(registry.get("delivery.dynamodb.pool.max").gauge().value()));
            assertTrue(Double.isNaN(registry.get("delivery.dynamodb.pool.pending").gauge().value()));
            var call = MetricCollector.create("ApiCall");
            call.reportMetric(CoreMetric.OPERATION_NAME, "PutItem");
            var first = call.createChild("Attempt");
            first.reportMetric(HttpMetric.CONCURRENCY_ACQUIRE_DURATION, Duration.ofMillis(200));
            first.reportMetric(CoreMetric.SERVICE_CALL_DURATION, Duration.ofMillis(250));
            var second = call.createChild("Attempt");
            second.reportMetric(HttpMetric.CONCURRENCY_ACQUIRE_DURATION, Duration.ofMillis(10));
            second.reportMetric(CoreMetric.SERVICE_CALL_DURATION, Duration.ofMillis(30));
            second.reportMetric(HttpMetric.MAX_CONCURRENCY, 64);
            second.reportMetric(HttpMetric.LEASED_CONCURRENCY, 60);
            second.reportMetric(HttpMetric.AVAILABLE_CONCURRENCY, 4);
            second.reportMetric(HttpMetric.PENDING_CONCURRENCY_ACQUIRES, 5);

            publisher.publish(call.collect());

            var acquire = registry.get("delivery.dynamodb.connection.acquire").tag("operation", "put_item").timer();
            assertEquals(2, acquire.count());
            assertEquals(210, acquire.totalTime(TimeUnit.MILLISECONDS), .001);
            assertEquals(280, registry.get("delivery.dynamodb.service.call").tag("operation", "put_item")
                    .timer().totalTime(TimeUnit.MILLISECONDS), .001);
            assertEquals(64, registry.get("delivery.dynamodb.pool.max").gauge().value());
            assertEquals(60, registry.get("delivery.dynamodb.pool.leased").gauge().value());
            assertEquals(4, registry.get("delivery.dynamodb.pool.available").gauge().value());
            assertEquals(5, registry.get("delivery.dynamodb.pool.pending").gauge().value());
            publisher.close();
            assertDoesNotThrow(() -> registry.counter("application.still.owns.registry").increment());
        } finally {
            registry.close();
        }
    }
}

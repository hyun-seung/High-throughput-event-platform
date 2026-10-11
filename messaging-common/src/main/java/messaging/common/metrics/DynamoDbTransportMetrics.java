package messaging.common.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** SDK attempt timings; pool gauges are the last reported observation, not a continuous sample. */
public final class DynamoDbTransportMetrics implements MetricPublisher {
    private static final Logger log = LoggerFactory.getLogger(DynamoDbTransportMetrics.class);
    private final MeterRegistry registry;
    private final AtomicInteger max = new AtomicInteger(-1);
    private final AtomicInteger leased = new AtomicInteger(-1);
    private final AtomicInteger available = new AtomicInteger(-1);
    private final AtomicInteger pending = new AtomicInteger(-1);
    private final ConcurrentMap<String, Timings> timings = new ConcurrentHashMap<>();
    private record Timings(Timer acquire, Timer service) { }

    public DynamoDbTransportMetrics(MeterRegistry registry) {
        this.registry = registry;
        gauge("max", max);
        gauge("leased", leased);
        gauge("available", available);
        gauge("pending", pending);
    }

    private void gauge(String state, AtomicInteger value) {
        registry.gauge("delivery.dynamodb.pool." + state, value,
                observed -> observed.get() < 0 ? Double.NaN : observed.get());
    }

    @Override
    public void publish(MetricCollection collection) {
        try {
            String operation = DynamoDbMetricsInterceptor.operation(
                    collection.metricValues(CoreMetric.OPERATION_NAME).stream().findFirst().orElse("unknown"));
            record(collection, operation);
        } catch (RuntimeException failure) {
            // SDK metric publication must not turn a successful business call into a failure.
            log.debug("DynamoDB transport metric collection failed", failure);
        }
    }

    private void record(MetricCollection collection, String operation) {
        var meters = timings.computeIfAbsent(operation, op -> new Timings(
                Timer.builder("delivery.dynamodb.connection.acquire").tag("operation", op).register(registry),
                Timer.builder("delivery.dynamodb.service.call").tag("operation", op).register(registry)));
        for (var duration : collection.metricValues(HttpMetric.CONCURRENCY_ACQUIRE_DURATION)) {
            meters.acquire().record(duration);
        }
        for (var duration : collection.metricValues(CoreMetric.SERVICE_CALL_DURATION)) {
            meters.service().record(duration);
        }
        collection.metricValues(HttpMetric.MAX_CONCURRENCY).forEach(max::set);
        collection.metricValues(HttpMetric.LEASED_CONCURRENCY).forEach(leased::set);
        collection.metricValues(HttpMetric.AVAILABLE_CONCURRENCY).forEach(available::set);
        collection.metricValues(HttpMetric.PENDING_CONCURRENCY_ACQUIRES).forEach(pending::set);
        collection.children().forEach(child -> record(child, operation));
    }

    @Override
    public void close() {
        // MeterRegistry belongs to the application; this publisher owns no I/O or worker thread.
    }
}

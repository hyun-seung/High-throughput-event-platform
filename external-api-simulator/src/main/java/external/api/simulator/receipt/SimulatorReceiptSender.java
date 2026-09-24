package external.api.simulator.receipt;

import external.api.simulator.delivery.dto.ProviderDispatchRequest;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.json.JsonMapper;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/** Bounded, process-local test traffic. This is deliberately not a durable provider outbox. */
@Service
@EnableConfigurationProperties(SimulatorReceiptProperties.class)
public class SimulatorReceiptSender implements AutoCloseable {
    private static final Set<String> CODES = Set.of("DELIVERED", "RETRY_1S", "RETRY_10S", "FALLBACK", "REJECTED", "UNKNOWN", "NONE");
    private final SimulatorReceiptProperties properties;
    private final JsonMapper mapper;
    private final MeterRegistry metrics;
    private final HttpClient client;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final ConcurrentHashMap<String, Boolean> tracked = new ConcurrentHashMap<>();
    private final Semaphore tracking, pending;

    public SimulatorReceiptSender(SimulatorReceiptProperties properties, JsonMapper mapper, MeterRegistry metrics) {
        this.properties = properties; this.mapper = mapper; this.metrics = metrics;
        tracking = new Semaphore(properties.maxTracked()); pending = new Semaphore(properties.maxPending());
        client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(properties.timeoutMillis()))
                .followRedirects(HttpClient.Redirect.NEVER).build();
        Gauge.builder("simulator.receipt.pending", pending, s -> properties.maxPending() - s.availablePermits()).register(metrics);
        Gauge.builder("simulator.receipt.tracked", tracked, Map::size).register(metrics);
    }

    /** Validate scenarios before recording any provider effect. Route 2 never inherits route 1 controls. */
    public Plan plan(boolean secondary, String attemptId, ProviderDispatchRequest request) {
        if (!properties.enabled()) return null;
        try {
            if (!UUID.fromString(attemptId).toString().equals(attemptId)
                    || !UUID.fromString(request.deliveryId()).toString().equals(request.deliveryId())
                    || request.invocation() == null || request.invocation() < 1 || request.invocation() > 4)
                throw new IllegalArgumentException();
            String prefix = secondary ? "simulatorTcpReceipt" : "simulatorReceipt";
            Map<String, Object> payload = request.payload();
            Object raw = payload.getOrDefault(prefix + "Codes", List.of("DELIVERED"));
            if (!(raw instanceof List<?> codes) || codes.isEmpty() || codes.size() > 4
                    || codes.stream().anyMatch(c -> !(c instanceof String) || !CODES.contains(c)))
                throw new IllegalArgumentException();
            String code = (String) codes.get(Math.min(request.invocation() - 1, codes.size() - 1));
            return new Plan(secondary, attemptId, request.deliveryId(), request.invocation(), code,
                    integer(payload, prefix + "DelayMillis", 100, 0, 300000),
                    integer(payload, prefix + "Copies", 1, 1, 5),
                    integer(payload, prefix + "CopyGapMillis", 100, 0, 60000),
                    integer(payload, secondary ? "simulatorTcpResponseDelayMillis" : "simulatorResponseDelayMillis", 0, 0, 60000));
        } catch (RuntimeException invalid) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid simulator receipt scenario");
        }
    }

    private static int integer(Map<String, Object> payload, String key, int fallback, int min, int max) {
        Object value = payload.getOrDefault(key, fallback);
        if (!(value instanceof Number number) || number.doubleValue() != number.intValue()
                || number.intValue() < min || number.intValue() > max) throw new IllegalArgumentException();
        return number.intValue();
    }

    /** Called only after an accepted provider effect, including the deliberate lost TCP reply scenario. */
    public void accepted(Plan plan) {
        if (plan == null) return;
        String id = UUID.nameUUIDFromBytes(("simulator-receipt:" + plan.secondary() + ":" + plan.attemptId()
                + ":" + plan.invocation()).getBytes(StandardCharsets.UTF_8)).toString();
        tracked.computeIfAbsent(id, ignored -> {
            if (!tracking.tryAcquire()) { count("tracking_full"); throw unavailable(); }
            if (plan.code().equals("NONE")) { count("suppressed"); return true; }
            if (!pending.tryAcquire()) { tracking.release(); count("queue_full"); throw unavailable(); }
            try {
                // One immutable body for duplicates and transport retries, including occurredAt.
                byte[] body = mapper.writeValueAsBytes(Map.of("receiptId", id, "deliveryId", plan.deliveryId(),
                        "attemptId", plan.attemptId(), "invocation", plan.invocation(), "code", plan.code(),
                        "outcome", plan.code().equals("DELIVERED") ? "DELIVERED" : "FAILED", "occurredAt", Instant.now().toString()));
                workers.submit(() -> deliver(plan, body));
                count("scheduled");
                return true;
            } catch (RuntimeException failure) { pending.release(); tracking.release(); throw failure; }
        });
    }

    private void deliver(Plan plan, byte[] body) {
        try {
            Thread.sleep(plan.delayMillis());
            for (int copy = 0; copy < plan.copies(); copy++) {
                if (copy > 0) Thread.sleep(plan.copyGapMillis());
                var request = HttpRequest.newBuilder(plan.secondary() ? properties.secondaryUrl() : properties.primaryUrl())
                        .timeout(Duration.ofMillis(properties.timeoutMillis()))
                        .header("Authorization", "Bearer " + (plan.secondary() ? properties.secondarySecret() : properties.primarySecret()))
                        .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
                for (int attempt = 1; attempt <= properties.maxAttempts(); attempt++) {
                    int status = 0;
                    count("http_attempt");
                    var response = client.sendAsync(request, HttpResponse.BodyHandlers.discarding());
                    try { status = response.get(properties.timeoutMillis(), TimeUnit.MILLISECONDS).statusCode(); }
                    catch (ExecutionException | TimeoutException transportFailure) { /* uncertain: reuse the exact body */ }
                    finally { if (!response.isDone()) response.cancel(true); }
                    if (status == 202) { count("acknowledged"); break; }
                    if (status >= 400 && status < 500 && status != 408 && status != 429) { count("rejected"); break; }
                    if (attempt == properties.maxAttempts()) { count("exhausted"); break; }
                    count("retry");
                    Thread.sleep(properties.retryDelayMillis());
                }
            }
        } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); count("cancelled"); }
        catch (RuntimeException failure) { count("failed"); }
        finally { pending.release(); }
    }

    public static void delayResponse(Plan plan) throws InterruptedException {
        if (plan != null) Thread.sleep(plan.responseDelayMillis());
    }
    private void count(String outcome) { metrics.counter("simulator.receipt.events", "outcome", outcome).increment(); }
    private static ResponseStatusException unavailable() {
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Simulator receipt capacity reached");
    }
    @PreDestroy @Override public void close() { workers.shutdownNow(); client.shutdownNow(); }

    public record Plan(boolean secondary, String attemptId, String deliveryId, int invocation, String code,
                       int delayMillis, int copies, int copyGapMillis, int responseDelayMillis) { }
}

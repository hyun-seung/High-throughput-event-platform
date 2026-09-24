package external.api.simulator.delivery.service;

import external.api.simulator.delivery.config.SimulatorProperties;
import external.api.simulator.delivery.dto.ProviderDispatchRequest;
import external.api.simulator.delivery.dto.ProviderDispatchResponse;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/** Process-local test evidence. No eviction: running out of tracking space must be visible. */
@Service
@EnableConfigurationProperties(SimulatorProperties.class)
public class SimulatorLedger {
    private static final Set<String> RESULT_CODES = Set.of("ACCEPTED", "RETRY_1S", "RETRY_10S", "FALLBACK", "REJECTED");
    private final SimulatorProperties properties;
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final Semaphore capacity;
    private final Counter calls;
    private final Counter effects;
    private final Counter deduplicated;
    private final Counter forcedFailures;
    private final Counter capacityRejections;

    public SimulatorLedger(SimulatorProperties properties, MeterRegistry registry) {
        this.properties = properties;
        capacity = new Semaphore(properties.maxTrackedKeys());
        calls = registry.counter("simulator.calls");
        effects = registry.counter("simulator.effects");
        deduplicated = registry.counter("simulator.deduplicated");
        forcedFailures = registry.counter("simulator.forced.failures");
        capacityRejections = registry.counter("simulator.capacity.rejections");
        Gauge.builder("simulator.tracked.keys", entries, Map::size).register(registry);
    }

    public ProviderDispatchResponse receive(String key, ProviderDispatchRequest request) {
        calls.increment();
        if (key.isBlank() || key.length() > 256 || request.deliveryId() == null || request.deliveryId().isBlank()
                || request.deliveryId().length() > 256 || request.payload() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid simulator request");
        }
        Object selectedCode = request.payload().getOrDefault("simulatorResultCode", "ACCEPTED");
        if (!(selectedCode instanceof String code) || !RESULT_CODES.contains(code)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid simulator result code");
        }
        Entry entry = entries.computeIfAbsent(key, ignored -> {
            if (!capacity.tryAcquire()) {
                capacityRejections.increment();
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Simulator tracking capacity reached");
            }
            return new Entry();
        });
        synchronized (entry) {
            entry.calls++;
            if (Boolean.TRUE.equals(request.payload().get("forceFail"))) {
                forcedFailures.increment();
                return new ProviderDispatchResponse(request.deliveryId(), false, Instant.now());
            }
            if (!"ACCEPTED".equals(code)) {
                forcedFailures.increment();
                return new ProviderDispatchResponse(request.deliveryId(), false, Instant.now(), code);
            }
            if (properties.deduplicate() && entry.firstResponse != null) {
                deduplicated.increment();
                return entry.firstResponse;
            }
            var response = new ProviderDispatchResponse(request.deliveryId(), true, Instant.now());
            if (entry.firstResponse == null) entry.firstResponse = response;
            entry.effects++;
            effects.increment();
            return response;
        }
    }

    public Map<String, Object> summary() {
        return Map.of("calls", calls.count(), "effects", effects.count(), "trackedKeys", entries.size(),
                "deduplicated", deduplicated.count(), "forcedFailures", forcedFailures.count(),
                "capacityRejections", capacityRejections.count(), "deduplicate", properties.deduplicate(),
                "responseDelayMillis", properties.responseDelayMillis(), "maxTrackedKeys", properties.maxTrackedKeys());
    }

    public Map<String, Long> counts(String key) {
        Entry entry = entries.get(key);
        if (entry == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Untracked simulator key");
        synchronized (entry) { return Map.of("calls", entry.calls, "effects", entry.effects); }
    }

    private static final class Entry {
        long calls;
        long effects;
        ProviderDispatchResponse firstResponse;
    }
}

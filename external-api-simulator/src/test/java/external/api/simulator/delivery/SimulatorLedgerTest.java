package external.api.simulator.delivery;

import external.api.simulator.delivery.config.SimulatorProperties;
import external.api.simulator.delivery.controller.DeliveryProviderController;
import external.api.simulator.delivery.dto.ProviderDispatchRequest;
import external.api.simulator.delivery.service.SimulatorLedger;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class SimulatorLedgerTest {
    private final ProviderDispatchRequest request = new ProviderDispatchRequest("delivery-1", 1L, "EMAIL", Map.of(), Instant.now());

    @ParameterizedTest
    @CsvSource({"RETRY_1S,429", "RETRY_10S,429", "FALLBACK,503", "REJECTED,400"})
    void selectedFailureHasNoEffectAndNextSuccessfulCallStillWorks(String code, int status) {
        var properties = new SimulatorProperties(0, true, 1);
        var ledger = new SimulatorLedger(properties, new SimpleMeterRegistry());
        var controller = new DeliveryProviderController(ledger, properties, org.mockito.Mockito.mock(external.api.simulator.receipt.SimulatorReceiptSender.class));
        var failed = new ProviderDispatchRequest("delivery-1", 1L, "EMAIL", Map.of("simulatorResultCode", code), Instant.now());
        var response = controller.receive("key", failed);
        assertEquals(status, response.getStatusCode().value());
        assertEquals(code, response.getBody().code());
        assertFalse(response.getBody().accepted());
        assertEquals(Map.of("calls", 1L, "effects", 0L), ledger.counts("key"));
        assertTrue(controller.receive("key", request).getBody().accepted());
        assertEquals(Map.of("calls", 2L, "effects", 1L), ledger.counts("key"));
    }

    @Test
    void invalidScenarioCannotCreateAnEffectOrConsumeTrackingCapacity() {
        var ledger = new SimulatorLedger(new SimulatorProperties(0, false, 1), new SimpleMeterRegistry());
        var invalid = new ProviderDispatchRequest("delivery-1", 1L, "EMAIL", Map.of("simulatorResultCode", "unexpected"), Instant.now());
        assertEquals(400, assertThrows(ResponseStatusException.class, () -> ledger.receive("invalid", invalid)).getStatusCode().value());
        assertTrue(ledger.receive("valid", request).accepted());
        assertEquals(1, ledger.summary().get("trackedKeys"));
    }

    @Test
    void disabledDeduplicationMakesEveryConcurrentCallVisibleAsAnEffect() throws Exception {
        checkConcurrentCalls(false, 20);
    }

    @Test
    void enabledDeduplicationStillCountsEveryCallButCreatesOneEffect() throws Exception {
        checkConcurrentCalls(true, 1);
    }

    private void checkConcurrentCalls(boolean deduplicate, long expectedEffects) throws Exception {
        var registry = new SimpleMeterRegistry();
        var ledger = new SimulatorLedger(new SimulatorProperties(0, deduplicate, 100), registry);
        var gate = new CountDownLatch(1);
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new ArrayList<Future<?>>();
            for (int i = 0; i < 20; i++) tasks.add(pool.submit(() -> {
                gate.await();
                ledger.receive("same-key", request);
                return null;
            }));
            gate.countDown();
            for (var task : tasks) task.get();
        }
        assertEquals(Map.of("calls", 20L, "effects", expectedEffects), ledger.counts("same-key"));
        assertEquals(20, registry.get("simulator.calls").counter().count());
        assertEquals(expectedEffects, registry.get("simulator.effects").counter().count());
        assertEquals(20 - expectedEffects, registry.get("simulator.deduplicated").counter().count());
        assertEquals(1, registry.get("simulator.tracked.keys").gauge().value());
        assertTrue(registry.getMeters().stream().allMatch(m -> m.getId().getTags().isEmpty()));
    }

    @Test
    void capacityRejectionDoesNotEvictEvidenceOrPreventExistingKeyObservation() {
        var ledger = new SimulatorLedger(new SimulatorProperties(0, false, 1), new SimpleMeterRegistry());
        ledger.receive("first", request);
        var failure = assertThrows(ResponseStatusException.class, () -> ledger.receive("overflow", request));
        assertEquals(503, failure.getStatusCode().value());
        ledger.receive("first", request);
        assertEquals(Map.of("calls", 2L, "effects", 2L), ledger.counts("first"));
        assertEquals(1.0, ledger.summary().get("capacityRejections"));
        assertEquals(3.0, ledger.summary().get("calls"));
    }

    @Test
    void forcedFailureCountsCallWithoutEffectAndDoesNotPoisonDeduplication() {
        var ledger = new SimulatorLedger(new SimulatorProperties(0, true, 1), new SimpleMeterRegistry());
        var rejected = new ProviderDispatchRequest("delivery-1", 1L, "EMAIL", Map.of("forceFail", true), Instant.now());
        assertFalse(ledger.receive("key", rejected).accepted());
        assertTrue(ledger.receive("key", request).accepted());
        assertEquals(Map.of("calls", 2L, "effects", 1L), ledger.counts("key"));
        assertEquals(1.0, ledger.summary().get("forcedFailures"));
    }

    @Test
    void configuredResponseDelayOccursAfterRecordingEffect() throws Exception {
        var properties = new SimulatorProperties(200, false, 10);
        var ledger = new SimulatorLedger(properties, new SimpleMeterRegistry());
        var controller = new DeliveryProviderController(ledger, properties, org.mockito.Mockito.mock(external.api.simulator.receipt.SimulatorReceiptSender.class));
        long start = System.nanoTime();
        assertTrue(controller.receive("key", request).getBody().accepted());
        assertTrue(System.nanoTime() - start >= 200_000_000L);
        assertEquals(Map.of("calls", 1L, "effects", 1L), ledger.counts("key"));
    }
}

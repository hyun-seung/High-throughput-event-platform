package event.delivery.dispatch.service;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryIds;
import event.common.metrics.DeliveryMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import event.delivery.dispatch.model.DispatchAttempt;
import event.delivery.dispatch.model.DispatchClaim;
import event.delivery.dispatch.port.DeliveryProviderClient;
import event.delivery.dispatch.port.DispatchAttemptStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DispatchServiceTest {

    private static final Instant NOW = Instant.parse("2026-09-22T00:00:00Z");
    private static final String PROVIDER = "mock-provider";

    private FakeDispatchAttemptStore attemptStore;
    private FakeDeliveryProviderClient providerClient;
    private DispatchService dispatchService;
    private DeliveryEvent event;
    private String attemptId;
    private SimpleMeterRegistry registry;

    @BeforeEach
    void setUp() {
        attemptStore = new FakeDispatchAttemptStore();
        providerClient = new FakeDeliveryProviderClient();
        registry = new SimpleMeterRegistry();
        DispatchProperties properties = new DispatchProperties(PROVIDER, Duration.ofSeconds(30));
        dispatchService = new DispatchService(
                attemptStore,
                providerClient,
                properties,
                Clock.fixed(NOW, ZoneOffset.UTC), new DeliveryMetrics(registry)
        );
        event = DeliveryEvent.requested(
                DeliveryIds.deliveryId(10L, "request-1"),
                10L,
                "ORDER_COMPLETED",
                Map.of("orderId", "100"),
                NOW
        ).toDispatchRequested();
        attemptId = DeliveryIds.attemptId(event.deliveryId(), PROVIDER, 1, 1);
    }

    @Test
    void claimedAttemptCallsProviderAndRecordsAcceptedResult() {
        DispatchAttempt attempt = new DispatchAttempt(event.deliveryId(), attemptId, PROVIDER, 1L);
        attemptStore.nextClaim = DispatchClaim.claimed(attempt);
        providerClient.nextResponse = new ProviderDispatchResponse(
                event.deliveryId(), true, NOW.plusSeconds(1));

        dispatchService.dispatch(event);

        assertEquals(1, providerClient.callCount);
        assertEquals(attemptId, providerClient.lastIdempotencyKey);
        assertEquals(attempt, attemptStore.acceptedAttempt);
        assertEquals(providerClient.nextResponse.processedAt(), attemptStore.providerProcessedAt);
        assertEquals(1, outcome("dispatch_accepted"));
        assertEquals(1, registry.get("delivery.acceptance.latency").timer().count());
    }

    @Test
    void acceptedAttemptSkipsProviderCall() {
        attemptStore.nextClaim = DispatchClaim.alreadyAccepted();

        dispatchService.dispatch(event);

        assertEquals(0, providerClient.callCount);
        assertFalse(attemptStore.acceptedRecorded);
        assertEquals(1, outcome("dispatch_duplicate"));
        assertEquals(0, outcome("dispatch_accepted"));
    }

    @Test
    void activeLeaseDoesNotCallProviderAgain() {
        attemptStore.nextClaim = DispatchClaim.inProgress();

        assertThrows(DispatchAttemptInProgressException.class, () -> dispatchService.dispatch(event));

        assertEquals(0, providerClient.callCount);
        assertFalse(attemptStore.acceptedRecorded);
    }

    @Test
    void durableReviewStateSkipsProviderCall() {
        attemptStore.nextClaim = DispatchClaim.reviewRequired();

        dispatchService.dispatch(event);

        assertEquals(0, providerClient.callCount);
        assertFalse(attemptStore.acceptedRecorded);
        assertEquals(1, outcome("dispatch_review"));
        assertEquals(0, outcome("dispatch_accepted"));
    }

    @Test
    void successfulProviderCallWithFailedStorageIsNotCountedAsPersistedAcceptance() {
        attemptStore.nextClaim = DispatchClaim.claimed(new DispatchAttempt(event.deliveryId(), attemptId, PROVIDER, 1));
        providerClient.nextResponse = new ProviderDispatchResponse(event.deliveryId(), true, NOW);
        attemptStore.storeFailure = new IllegalStateException("storage unavailable");
        assertThrows(IllegalStateException.class, () -> dispatchService.dispatch(event));
        assertEquals(1, providerClient.callCount);
        assertEquals(0, outcome("dispatch_accepted"));
        assertEquals(0, registry.get("delivery.acceptance.latency").timer().count());
        assertEquals(1, registry.get("delivery.stage.duration")
                .tags("stage", "dispatch_store", "result", "failure").timer().count());
    }

    @Test
    void providerRejectionIsCountedAsHttpStageFailure() {
        attemptStore.nextClaim = DispatchClaim.claimed(new DispatchAttempt(event.deliveryId(), attemptId, PROVIDER, 1));
        providerClient.nextResponse = new ProviderDispatchResponse(event.deliveryId(), false, NOW);
        assertThrows(IllegalStateException.class, () -> dispatchService.dispatch(event));
        assertFalse(attemptStore.acceptedRecorded);
        assertEquals(0, outcome("dispatch_accepted"));
        assertEquals(1, registry.get("delivery.stage.duration")
                .tags("stage", "dispatch_http", "result", "failure").timer().count());
    }

    private double outcome(String name) {
        return registry.get("delivery.outcomes").tag("outcome", name).counter().count();
    }

    @Test
    void failedReviewHandoffEscapesWithoutCallingProvider() {
        attemptStore.claimFailure = new IllegalStateException("review state write unconfirmed");

        assertThrows(IllegalStateException.class, () -> dispatchService.dispatch(event));

        assertEquals(0, providerClient.callCount);
        assertFalse(attemptStore.acceptedRecorded);
    }

    private static final class FakeDispatchAttemptStore implements DispatchAttemptStore {

        private DispatchClaim nextClaim;
        private boolean acceptedRecorded;
        private DispatchAttempt acceptedAttempt;
        private Instant providerProcessedAt;
        private RuntimeException claimFailure;
        private RuntimeException storeFailure;

        @Override
        public DispatchClaim claim(
                DeliveryEvent event,
                String attemptId,
                String provider,
                Instant now,
                Instant leaseUntil
        ) {
            assertEquals(NOW, now);
            assertEquals(NOW.plusSeconds(30), leaseUntil);
            if (claimFailure != null) throw claimFailure;
            return nextClaim;
        }

        @Override
        public void markAccepted(DispatchAttempt attempt, Instant providerProcessedAt, Instant now) {
            if (storeFailure != null) throw storeFailure;
            acceptedRecorded = true;
            acceptedAttempt = attempt;
            this.providerProcessedAt = providerProcessedAt;
            assertEquals(NOW, now);
        }
    }

    private static final class FakeDeliveryProviderClient implements DeliveryProviderClient {

        private int callCount;
        private String lastIdempotencyKey;
        private ProviderDispatchResponse nextResponse;

        @Override
        public ProviderDispatchResponse send(DeliveryEvent event, String idempotencyKey) {
            callCount++;
            lastIdempotencyKey = idempotencyKey;
            return nextResponse;
        }
    }
}

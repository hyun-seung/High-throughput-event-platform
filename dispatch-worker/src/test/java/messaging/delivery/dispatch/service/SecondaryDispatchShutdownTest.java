package messaging.delivery.dispatch.service;

import messaging.common.delivery.DeliveryEvent;
import messaging.common.delivery.DeliveryIds;
import messaging.common.metrics.DeliveryMetrics;
import messaging.delivery.dispatch.config.DispatchProperties;
import messaging.delivery.dispatch.config.SecondaryDispatchProperties;
import messaging.delivery.dispatch.external.client.ProviderFailureException;
import messaging.delivery.dispatch.model.DispatchAttempt;
import messaging.delivery.dispatch.port.DispatchAttemptStore;
import messaging.delivery.dispatch.port.SecondaryProviderClient;
import messaging.delivery.dispatch.retry.RetryPublisher;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class SecondaryDispatchShutdownTest {
    @Test
    void shutdownTimeUnknownTcpReplyKeepsClaimWithoutPublishingRetry() {
        var now = Instant.parse("2026-09-26T00:00:00Z");
        var event = DeliveryEvent.requested(DeliveryIds.deliveryId(10L, "tcp-shutdown"), 10L,
                "ORDER_COMPLETED", Map.of("orderId", "100"), now).toDispatchRequested();
        var attempt = new DispatchAttempt(event.deliveryId(), "secondary-attempt", "tcp-provider", 1, 0,
                now.plusSeconds(90), 2);
        var store = mock(DispatchAttemptStore.class);
        var retries = mock(RetryPublisher.class);
        SecondaryProviderClient client = (request, key) -> {
            throw new ProviderFailureException(ProviderFailureException.Kind.NO_RESPONSE);
        };
        var service = new SecondaryDispatchService(store, client, new SecondaryDispatchProperties(null, null),
                new DispatchProperties(null, Duration.ofSeconds(30)), Clock.fixed(now, ZoneOffset.UTC),
                new DeliveryMetrics(new SimpleMeterRegistry()));
        service.retryPublisher(retries);
        service.onContextClosed();

        assertDoesNotThrow(() -> service.invokeRetry(event, attempt));

        verifyNoInteractions(store, retries);
    }
}

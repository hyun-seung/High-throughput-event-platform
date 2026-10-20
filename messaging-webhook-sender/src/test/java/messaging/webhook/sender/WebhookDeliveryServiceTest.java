package messaging.webhook.sender;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class WebhookDeliveryServiceTest {
    private final WebhookBatchRepository repository = mock(WebhookBatchRepository.class);
    private final CustomerWebhookClient client = mock(CustomerWebhookClient.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final AtomicLong nanos = new AtomicLong();
    private final WebhookSenderProperties.Destination destination = new WebhookSenderProperties.Destination(
            URI.create("https://example.org/hook"), false);
    private final WebhookSenderProperties properties = new WebhookSenderProperties(true, 100, 1,
            4, Duration.ofMillis(100), Duration.ZERO, 262144, Duration.ofSeconds(3), Duration.ofSeconds(30),
            Duration.ofSeconds(1), Duration.ofSeconds(60), Map.of(42L, destination));
    private final WebhookDeliveryService service = new WebhookDeliveryService(repository, client, properties, meters, nanos::get);
    private final WebhookBatchRepository.Claim claim = claim(1);

    private WebhookBatchRepository.Claim claim(int attempt) {
        return new WebhookBatchRepository.Claim(UUID.randomUUID(), 42, destination.url().toString(),
                "{}", 100, attempt, UUID.randomUUID());
    }

    private void available(WebhookBatchRepository.Claim value) {
        when(repository.claim(42, destination, properties)).thenReturn(Optional.of(value));
        when(repository.owns(value)).thenReturn(true);
        when(client.send(value, destination)).thenReturn(new CustomerWebhookClient.Result(true, 204, null));
        when(repository.complete(eq(value), any(), any())).thenReturn(true);
    }

    @Test
    void continuesAfterCommittedAcknowledgmentsAndStopsOnAnEmptyQueue() {
        available(claim);
        when(repository.claim(42, destination, properties))
                .thenReturn(Optional.of(claim), Optional.of(claim), Optional.empty());

        assertFalse(service.deliver(42));

        var order = inOrder(repository, client);
        for (int batch = 0; batch < 2; batch++) {
            order.verify(repository).claim(42, destination, properties);
            order.verify(repository).owns(claim);
            order.verify(client).send(claim, destination);
            order.verify(repository).complete(eq(claim), any(), eq(Duration.ofSeconds(1)));
        }
        order.verify(repository).claim(42, destination, properties);
        assertEquals(2, meters.counter("messaging.webhook.attempts", "outcome", "delivered").count());
    }

    @Test
    void yieldsAfterTheBatchLimitAndSignalsContinuation() {
        available(claim);
        assertTrue(service.deliver(42));
        verify(repository, times(4)).claim(42, destination, properties);
        verify(client, times(4)).send(claim, destination);
    }

    @Test
    void finishesTheClaimedBatchButYieldsBeforeAnotherClaimAfterTheTimeBudget() {
        available(claim);
        when(client.send(claim, destination)).thenAnswer(call -> {
            nanos.set(Duration.ofMillis(100).toNanos());
            return new CustomerWebhookClient.Result(true, 204, null);
        });
        assertTrue(service.deliver(42));
        verify(repository).claim(42, destination, properties);
        verify(repository).complete(eq(claim), any(), any());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 21})
    void anUnacknowledgedOrExhaustedBatchStopsTheTurnWithItsExistingBackoff(int attempt) {
        var reserved = claim(attempt);
        available(reserved);
        var failure = new CustomerWebhookClient.Result(false, 503, "HTTP_503");
        when(client.send(reserved, destination)).thenReturn(failure);

        assertFalse(service.deliver(42));

        verify(repository).claim(42, destination, properties);
        verify(repository).complete(reserved, failure, properties.retryAfter(attempt));
        assertEquals(1, meters.counter("messaging.webhook.attempts", "outcome", attempt == 21 ? "exhausted" : "retry").count());
    }

    @Test
    void lostOwnershipDoesNotSendOrContinue() {
        available(claim);
        when(repository.owns(claim)).thenReturn(false);
        assertFalse(service.deliver(42));
        verifyNoInteractions(client);
        verify(repository, never()).complete(any(), any(), any());
    }

    @Test
    void rejectedCompletionDoesNotContinueOrCountAnAcknowledgmentAsDelivered() {
        available(claim);
        when(repository.complete(eq(claim), any(), any())).thenReturn(false);
        assertFalse(service.deliver(42));
        verify(repository).claim(42, destination, properties);
        assertNull(meters.find("messaging.webhook.attempts").counter());
    }

    @Test
    void anInterruptedWorkerDoesNotStartAnotherBatch() {
        available(claim);
        when(client.send(claim, destination)).thenAnswer(call -> {
            Thread.currentThread().interrupt();
            return new CustomerWebhookClient.Result(true, 204, null);
        });
        try {
            assertFalse(service.deliver(42));
            assertTrue(Thread.currentThread().isInterrupted());
            verify(repository).complete(eq(claim), any(), any());
            verify(repository).claim(42, destination, properties);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void anUnknownCustomerDoesNotStartWork() {
        assertFalse(service.deliver(999));
        verifyNoInteractions(repository, client);
    }

    @Test
    void repositoryFailurePropagatesWithoutSending() {
        when(repository.claim(42, destination, properties)).thenThrow(new IllegalStateException("SQL unavailable"));
        assertThrows(IllegalStateException.class, () -> service.deliver(42));
        verifyNoInteractions(client);
    }
}

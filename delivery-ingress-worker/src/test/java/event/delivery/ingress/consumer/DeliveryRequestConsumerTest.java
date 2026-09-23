package event.delivery.ingress.consumer;

import event.common.delivery.DeliveryEvent;
import event.delivery.ingress.repository.DeliveryRepository;
import event.delivery.ingress.service.DeliveryFlowProducer;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DeliveryRequestConsumerTest {

    private final DeliveryRepository repository = mock(DeliveryRepository.class);
    private final DeliveryFlowProducer producer = mock(DeliveryFlowProducer.class);
    private final DeliveryRequestConsumer consumer = new DeliveryRequestConsumer(repository, producer);
    private final DeliveryEvent first = DeliveryEvent.requested(
            "delivery-1", 10L, "EMAIL", Map.of("body", "hello"), Instant.parse("2026-09-23T00:00:00Z"));

    @Test
    void duplicateForwardsPersistedIngressTimeInsteadOfClientRetryTime() {
        var retry = DeliveryEvent.requested(first.deliveryId(), first.tenantId(), first.deliveryType(),
                first.payload(), first.occurredAt().plusSeconds(3600));
        when(repository.saveOrLoad(retry)).thenReturn(first);
        when(producer.sendDispatchRequested(any())).thenReturn(CompletableFuture.completedFuture(null));

        consumer.consume(retry);

        verify(producer).sendDispatchRequested(first.toDispatchRequested());
    }

    @Test
    void storageFailureDoesNotPublishDispatch() {
        when(repository.saveOrLoad(first)).thenThrow(new IllegalStateException("storage unavailable"));

        assertThrows(IllegalStateException.class, () -> consumer.consume(first));

        verifyNoInteractions(producer);
    }

    @Test
    void dispatchPublishFailureEscapesListenerSoInputCanBeReplayed() {
        when(repository.saveOrLoad(first)).thenReturn(first);
        when(producer.sendDispatchRequested(any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("ack missing")));

        assertThrows(CompletionException.class, () -> consumer.consume(first));
    }
}

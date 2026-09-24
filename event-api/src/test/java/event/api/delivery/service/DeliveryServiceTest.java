package event.api.delivery.service;

import event.api.delivery.dto.DeliveryRequest;
import event.api.delivery.dto.DeliveryResponse;
import event.api.delivery.kafka.DeliveryEventPublisher;
import event.api.security.principal.AuthenticatedUser;
import event.common.delivery.DeliveryEvent;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.SendResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
class DeliveryServiceTest {

    @Test
    void acceptanceCompletesOnlyAfterKafkaAck() {
        CompletableFuture<SendResult<String, DeliveryEvent>> kafkaResult = new CompletableFuture<>();
        CapturingPublisher publisher = new CapturingPublisher(kafkaResult);

        DeliveryService service = new DeliveryService(publisher, metrics());
        CompletableFuture<DeliveryResponse> acceptance = service.accept(
                new AuthenticatedUser(10L, "tenant"),
                "client-request-1",
                new DeliveryRequest("ORDER_COMPLETED", Map.of("orderId", "100"))
        );

        assertFalse(acceptance.isDone());

        kafkaResult.complete(null);

        assertTrue(acceptance.isDone());
        assertEquals("ACCEPTED", acceptance.join().status());
    }

    @Test
    void sameClientKeyCreatesSameDeliveryId() {
        CapturingPublisher publisher = new CapturingPublisher(CompletableFuture.completedFuture(null));
        DeliveryService service = new DeliveryService(publisher, metrics());
        AuthenticatedUser user = new AuthenticatedUser(10L, "tenant");
        DeliveryRequest request = new DeliveryRequest("ORDER_COMPLETED", Map.of("orderId", "100"));

        DeliveryResponse first = service.accept(user, "client-request-1", request).join();
        DeliveryResponse second = service.accept(user, "client-request-1", request).join();

        assertEquals(first.deliveryId(), second.deliveryId());

        assertEquals(publisher.events.get(0).eventId(), publisher.events.get(1).eventId());
    }

    @Test
    void failedKafkaAckDoesNotAcceptAndRetainsTheCause() {
        var kafkaResult = new CompletableFuture<SendResult<String, DeliveryEvent>>();
        var service = new DeliveryService(new CapturingPublisher(kafkaResult), metrics());
        var acceptance = service.accept(new AuthenticatedUser(10L, "tenant"), "request-1",
                new DeliveryRequest("EMAIL", Map.of()));
        var failure = new org.apache.kafka.common.errors.TimeoutException("ack was not observed");

        kafkaResult.completeExceptionally(failure);

        var completion = assertThrows(CompletionException.class, acceptance::join);
        var exception = assertInstanceOf(DeliveryAcceptanceException.class, completion.getCause());
        assertSame(failure, exception.getCause());
    }

    @Test
    void synchronousSendFailureHasTheSameUnconfirmedAcceptanceContract() {
        var failure = new org.apache.kafka.common.errors.TimeoutException("metadata unavailable");
        var service = new DeliveryService(event -> { throw failure; }, metrics());

        var acceptance = service.accept(new AuthenticatedUser(10L, "tenant"), "request-1",
                new DeliveryRequest("EMAIL", Map.of()));

        var completion = assertThrows(CompletionException.class, acceptance::join);
        var exception = assertInstanceOf(DeliveryAcceptanceException.class, completion.getCause());
        assertSame(failure, exception.getCause());
    }

    private static final class CapturingPublisher implements DeliveryEventPublisher {

        private final CompletableFuture<SendResult<String, DeliveryEvent>> result;
        private final List<DeliveryEvent> events = new ArrayList<>();

        private CapturingPublisher(CompletableFuture<SendResult<String, DeliveryEvent>> result) {
            this.result = result;
        }

        @Override
        public CompletableFuture<SendResult<String, DeliveryEvent>> send(DeliveryEvent event) {
            events.add(event);
            return result;
        }
    }
    private static event.common.metrics.DeliveryMetrics metrics() {
        return new event.common.metrics.DeliveryMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
    }

}

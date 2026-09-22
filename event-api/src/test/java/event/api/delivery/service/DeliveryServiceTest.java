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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
class DeliveryServiceTest {

    @Test
    void acceptanceCompletesOnlyAfterKafkaAck() {
        CompletableFuture<SendResult<String, DeliveryEvent>> kafkaResult = new CompletableFuture<>();
        CapturingPublisher publisher = new CapturingPublisher(kafkaResult);

        DeliveryService service = new DeliveryService(publisher);
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
        DeliveryService service = new DeliveryService(publisher);
        AuthenticatedUser user = new AuthenticatedUser(10L, "tenant");
        DeliveryRequest request = new DeliveryRequest("ORDER_COMPLETED", Map.of("orderId", "100"));

        DeliveryResponse first = service.accept(user, "client-request-1", request).join();
        DeliveryResponse second = service.accept(user, "client-request-1", request).join();

        assertEquals(first.deliveryId(), second.deliveryId());

        assertEquals(publisher.events.get(0).eventId(), publisher.events.get(1).eventId());
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
}

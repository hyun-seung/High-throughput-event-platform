package event.common.delivery;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class DeliveryEventTest {

    @Test
    void sameIdempotencyKeyProducesSameDeliveryAndEventIds() {
        String firstDeliveryId = DeliveryIds.deliveryId(10L, "client-request-1");
        String secondDeliveryId = DeliveryIds.deliveryId(10L, "client-request-1");

        DeliveryEvent first = DeliveryEvent.requested(
                firstDeliveryId, 10L, "ORDER_COMPLETED", Map.of("orderId", "100"), Instant.now());
        DeliveryEvent second = DeliveryEvent.requested(
                secondDeliveryId, 10L, "ORDER_COMPLETED", Map.of("orderId", "100"), Instant.now());

        assertEquals(first.deliveryId(), second.deliveryId());
        assertEquals(first.eventId(), second.eventId());
    }

    @Test
    void nextStageHasNewEventIdAndCausationId() {
        DeliveryEvent requested = DeliveryEvent.requested(
                DeliveryIds.deliveryId(10L, "client-request-1"),
                10L,
                "ORDER_COMPLETED",
                Map.of("orderId", "100"),
                Instant.now()
        );

        DeliveryEvent dispatchRequested = requested.toDispatchRequested();

        assertNotEquals(requested.eventId(), dispatchRequested.eventId());
        assertEquals(requested.eventId(), dispatchRequested.causationId());
        assertEquals(requested.correlationId(), dispatchRequested.correlationId());
        assertEquals(requested.occurredAt(), dispatchRequested.occurredAt());
    }

    @Test
    void payloadOrderIsCanonicalizedForIdempotencyComparison() {
        DeliveryEvent first = DeliveryEvent.requested(
                DeliveryIds.deliveryId(10L, "client-request-1"),
                10L,
                "ORDER_COMPLETED",
                Map.of("z", 1, "a", Map.of("y", 2, "b", 3)),
                Instant.now()
        );
        DeliveryEvent second = DeliveryEvent.requested(
                DeliveryIds.deliveryId(10L, "client-request-1"),
                10L,
                "ORDER_COMPLETED",
                Map.of("a", Map.of("b", 3, "y", 2), "z", 1),
                Instant.now()
        );

        assertEquals(first.payload(), second.payload());
        assertEquals(first.payload().keySet().stream().toList(), java.util.List.of("a", "z"));
    }
}

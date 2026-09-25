package event.common.lifecycle;

import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Stable immutable result. Kafka may contain more than one physical record with this eventId. */
public record DeliveryFinalized(int schemaVersion, String eventType, String eventId, String deliveryId,
        long tenantId, String deliveryType, String outcome, String reason, int routeOrder,
        String attemptId, String provider, Instant occurredAt, Instant resultAt, Instant finalizedAt, Instant deadline,
        String requestKey) {
    public DeliveryFinalized { requestKey = requestKey == null ? deliveryId : requestKey; }
    public DeliveryFinalized(int schemaVersion, String eventType, String eventId, String deliveryId,
            long tenantId, String deliveryType, String outcome, String reason, int routeOrder,
            String attemptId, String provider, Instant occurredAt, Instant resultAt, Instant finalizedAt, Instant deadline) {
        this(schemaVersion, eventType, eventId, deliveryId, tenantId, deliveryType, outcome, reason, routeOrder,
                attemptId, provider, occurredAt, resultAt, finalizedAt, deadline, deliveryId);
    }
    public static String eventId(String deliveryId) {
        return UUID.nameUUIDFromBytes(("finalized:" + deliveryId + ":v1").getBytes(StandardCharsets.UTF_8)).toString();
    }
}

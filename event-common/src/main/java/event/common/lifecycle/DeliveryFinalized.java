package event.common.lifecycle;

import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Stable immutable result. Kafka may contain more than one physical record with this eventId. */
public record DeliveryFinalized(int schemaVersion, String eventType, String eventId, String deliveryId,
        long tenantId, String deliveryType, String outcome, String reason, int routeOrder,
        String attemptId, String provider, Instant occurredAt, Instant resultAt, Instant finalizedAt, Instant deadline) {
    public static String eventId(String deliveryId) {
        return UUID.nameUUIDFromBytes(("finalized:" + deliveryId + ":v1").getBytes(StandardCharsets.UTF_8)).toString();
    }
}

package event.common.delivery;

import java.time.Instant;
import java.util.Map;

public record DeliveryEvent(
        int schemaVersion,
        String eventId,
        DeliveryEventType eventType,
        String deliveryId,
        Long tenantId,
        String deliveryType,
        Map<String, Object> payload,
        Instant occurredAt,
        String correlationId,
        String causationId,
        Boolean fallbackAllowed
) {
    public DeliveryEvent {
        fallbackAllowed = Boolean.TRUE.equals(fallbackAllowed);
    }

    private static final int SCHEMA_VERSION = 1;

    public static DeliveryEvent requested(
            String deliveryId,
            Long tenantId,
            String deliveryType,
            Map<String, Object> payload,
            Instant occurredAt
    ) {
        return requested(deliveryId, tenantId, deliveryType, payload, occurredAt, false);
    }

    public static DeliveryEvent requested(String deliveryId, Long tenantId, String deliveryType,
                                         Map<String, Object> payload, Instant occurredAt, boolean fallbackAllowed) {
        return new DeliveryEvent(
                SCHEMA_VERSION,
                DeliveryIds.eventId(deliveryId, DeliveryEventType.DELIVERY_REQUESTED),
                DeliveryEventType.DELIVERY_REQUESTED,
                deliveryId,
                tenantId,
                deliveryType,
                DeliveryPayloads.canonicalize(payload),
                occurredAt,
                deliveryId,
                null,
                fallbackAllowed
        );
    }

    public DeliveryEvent toDispatchRequested() {
        return new DeliveryEvent(
                SCHEMA_VERSION,
                DeliveryIds.eventId(deliveryId, DeliveryEventType.DISPATCH_REQUESTED),
                DeliveryEventType.DISPATCH_REQUESTED,
                deliveryId,
                tenantId,
                deliveryType,
                payload,
                occurredAt,
                correlationId,
                eventId,
                fallbackAllowed
        );
    }
}

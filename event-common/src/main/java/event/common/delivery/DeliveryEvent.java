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
        String causationId
) {

    private static final int SCHEMA_VERSION = 1;

    public static DeliveryEvent requested(
            String deliveryId,
            Long tenantId,
            String deliveryType,
            Map<String, Object> payload,
            Instant occurredAt
    ) {
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
                null
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
                eventId
        );
    }
}

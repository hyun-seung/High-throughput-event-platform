package event.processing.worker.external.dto;

import event.common.delivery.DeliveryEvent;

import java.time.Instant;
import java.util.Map;

public record ProviderDispatchRequest(
        String deliveryId,
        Long tenantId,
        String deliveryType,
        Map<String, Object> payload,
        Instant occurredAt
) {

    public static ProviderDispatchRequest from(DeliveryEvent event) {
        return new ProviderDispatchRequest(
                event.deliveryId(),
                event.tenantId(),
                event.deliveryType(),
                event.payload(),
                event.occurredAt()
        );
    }
}

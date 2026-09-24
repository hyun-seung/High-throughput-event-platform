package event.common.tcp;

import event.common.delivery.DeliveryEvent;
import java.time.Instant;
import java.util.Map;

public record TcpDeliveryRequest(String deliveryId, String attemptId, Long tenantId, String deliveryType,
                                 Map<String, Object> payload, Instant occurredAt) {
    public static TcpDeliveryRequest from(DeliveryEvent event, String attemptId) {
        return new TcpDeliveryRequest(event.deliveryId(), attemptId, event.tenantId(), event.deliveryType(), event.payload(), event.occurredAt());
    }
}

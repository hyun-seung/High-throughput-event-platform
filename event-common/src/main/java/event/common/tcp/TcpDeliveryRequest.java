package event.common.tcp;

import event.common.delivery.DeliveryEvent;
import java.time.Instant;
import java.util.Map;

public record TcpDeliveryRequest(String deliveryId, String attemptId, Long tenantId, String deliveryType,
                                 Map<String, Object> payload, Instant occurredAt, Integer invocation) {
    public TcpDeliveryRequest(String deliveryId, String attemptId, Long tenantId, String deliveryType,
                              Map<String, Object> payload, Instant occurredAt) {
        this(deliveryId, attemptId, tenantId, deliveryType, payload, occurredAt, 1);
    }
    public static TcpDeliveryRequest from(DeliveryEvent event, String attemptId) {
        return from(event, attemptId, 1);
    }
    public static TcpDeliveryRequest from(DeliveryEvent event, String attemptId, int invocation) {
        return new TcpDeliveryRequest(event.deliveryId(), attemptId, event.tenantId(), event.deliveryType(), event.payload(), event.occurredAt(), invocation);
    }
}

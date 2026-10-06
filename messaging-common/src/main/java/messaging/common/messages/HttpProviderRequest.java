package messaging.common.messages;

import messaging.common.delivery.DeliveryPayloads;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** The provider wire body prepared by PRE-SEND-MANAGER for every carrier. */
public record HttpProviderRequest(String clientMsgId, long tenantId, String deliveryType,
                                  String recipientNumber, Map<String, Object> payload,
                                  Instant occurredAt, int invocation) {
    public HttpProviderRequest {
        Objects.requireNonNull(clientMsgId);
        Objects.requireNonNull(deliveryType);
        Objects.requireNonNull(recipientNumber);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(occurredAt);
        if (clientMsgId.isBlank() || deliveryType.isBlank() || recipientNumber.isBlank() || invocation < 1) {
            throw new IllegalArgumentException("Invalid HTTP provider request");
        }
        payload = DeliveryPayloads.canonicalize(payload);
    }
}

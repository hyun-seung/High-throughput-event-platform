package messaging.common.messages;


import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/** The provider wire body prepared by PRE-SEND-MANAGER for every carrier. */
public record HttpProviderRequest(String clientMsgId, long tenantId, String deliveryType,
                                  String recipientNumber, Map<String, Object> payload,
                                  Instant occurredAt) {
    public HttpProviderRequest {
        Objects.requireNonNull(clientMsgId);
        Objects.requireNonNull(deliveryType);
        Objects.requireNonNull(recipientNumber);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(occurredAt);
        if (clientMsgId.isBlank() || clientMsgId.getBytes(StandardCharsets.UTF_8).length > 40
                || deliveryType.isBlank() || recipientNumber.isBlank()) {
            throw new IllegalArgumentException("Invalid HTTP provider request");
        }
        payload = MessagePayloads.canonicalize(payload);
    }
}

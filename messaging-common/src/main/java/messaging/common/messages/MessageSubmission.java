package messaging.common.messages;

import messaging.common.delivery.DeliveryPayloads;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Immutable customer message; executionId is generated once by MSG-RECEIVE-API. */
public record MessageSubmission(
        String executionId,
        long clientId,
        String messageId,
        String recipientNumber,
        MessageCategory messageCategory,
        Map<String, Object> payload,
        boolean fallbackAllowed,
        Instant receivedAt
) {
    public MessageSubmission {
        Objects.requireNonNull(executionId);
        Objects.requireNonNull(messageId);
        Objects.requireNonNull(recipientNumber);
        Objects.requireNonNull(messageCategory);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(receivedAt);
        payload = DeliveryPayloads.canonicalize(payload);
    }
}

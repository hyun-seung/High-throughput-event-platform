package messaging.common.messages;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Immutable customer message; clientMsgId is generated once by MSG-RECEIVE-API. */
public record MessageSubmission(
        String clientMsgId,
        long clientId,
        String messageId,
        String recipientNumber,
        MessageCategory messageCategory,
        Map<String, Object> payload,
        Map<String, Object> secondarySendPayload,
        Instant receivedAt
) {
    public MessageSubmission {
        Objects.requireNonNull(clientMsgId);
        if (clientMsgId.isBlank() || clientMsgId.getBytes(StandardCharsets.UTF_8).length > 40) {
            throw new IllegalArgumentException("clientMsgId is required (up to 40 UTF-8 bytes)");
        }
        Objects.requireNonNull(messageId);
        Objects.requireNonNull(recipientNumber);
        Objects.requireNonNull(messageCategory);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(receivedAt);
        payload = MessagePayloads.canonicalize(payload);
        if (secondarySendPayload != null) {
            if (secondarySendPayload.isEmpty()) throw new IllegalArgumentException("secondarySendPayload is empty");
            secondarySendPayload = MessagePayloads.canonicalize(secondarySendPayload);
        }
    }

    public boolean hasSecondarySendPayload() { return secondarySendPayload != null; }
}

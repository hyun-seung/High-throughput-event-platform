package messaging.common.tcp;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Provisional second-send wire request; clientMsgId is the only external message identifier. */
public record MessagingTcpRequest(String clientMsgId, Long clientId, String messageCategory,
                                  Map<String, Object> payload, Instant requestedAt) {
    public MessagingTcpRequest {
        Objects.requireNonNull(clientMsgId);
        Objects.requireNonNull(clientId);
        Objects.requireNonNull(messageCategory);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(requestedAt);
        if (clientMsgId.isBlank() || clientMsgId.getBytes(StandardCharsets.UTF_8).length > 40
                || messageCategory.isBlank()) {
            throw new IllegalArgumentException("Invalid messaging TCP request");
        }
    }
}

package messaging.common.tcp;

import java.time.Instant;

/** Immediate final result for the provisional second-send TCP protocol. */
public record MessagingTcpResponse(String clientMsgId, String status, Error error, Instant processedAt) {
    public record Error(Integer code, String message) { }
}

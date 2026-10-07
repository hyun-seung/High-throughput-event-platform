package messaging.common.messages;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Frozen immediate response from the provisional second-send TCP protocol. */
public record TcpSendResult(String resultId, String clientMsgId, String attemptId, String source,
                            Status status, Integer errorCode, String providerCode, Instant observedAt) {
    public enum Status { SUCCESS, FAILED, TIMEOUT, INVALID_RESPONSE }

    public TcpSendResult {
        Objects.requireNonNull(resultId);
        Objects.requireNonNull(clientMsgId);
        Objects.requireNonNull(attemptId);
        Objects.requireNonNull(source);
        Objects.requireNonNull(status);
        Objects.requireNonNull(observedAt);
        if (!"TCP_RESPONSE".equals(source) || !resultId.equals(id(clientMsgId, attemptId))
                || clientMsgId.isBlank() || attemptId.isBlank()
                || (status == Status.SUCCESS) != (errorCode == null)
                || errorCode != null && (errorCode < 10000 || errorCode > 79999)) {
            throw new IllegalArgumentException("Invalid TCP result");
        }
    }

    public static String id(String clientMsgId, String attemptId) {
        return UUID.nameUUIDFromBytes(("tcp-send-result:" + clientMsgId + ":" + attemptId)
                .getBytes(StandardCharsets.UTF_8)).toString();
    }
}

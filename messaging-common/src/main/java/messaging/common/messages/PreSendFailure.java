package messaging.common.messages;

import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/** A pre-send rejection handed to MSG-RESULT-MANAGER for finalization. */
public record PreSendFailure(String resultId, String clientMsgId, String source, Reason reason, Instant observedAt) {
    public enum Reason { CONTRACT_MISSING, CONTRACT_DISABLED, PRIMARY_EXPIRED }

    public PreSendFailure {
        Objects.requireNonNull(resultId);
        Objects.requireNonNull(clientMsgId);
        Objects.requireNonNull(source);
        Objects.requireNonNull(reason);
        Objects.requireNonNull(observedAt);
        if (resultId.isBlank() || clientMsgId.isBlank() || !"PRE_SEND".equals(source)) {
            throw new IllegalArgumentException("Invalid pre-send failure");
        }
    }

    public static PreSendFailure of(String clientMsgId, Reason reason, Instant observedAt) {
        String id = UUID.nameUUIDFromBytes(("pre-send-failure:" + clientMsgId)
                .getBytes(StandardCharsets.UTF_8)).toString();
        return new PreSendFailure(id, clientMsgId, "PRE_SEND", reason, observedAt);
    }
}

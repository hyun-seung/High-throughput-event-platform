package messaging.common.messages;

import java.time.Instant;
import java.util.Objects;

/** Final immediate response for one 2nd-send TCP attempt. */
public record SecondaryStageDecision(String decisionId, String clientMsgId, String attemptId,
                                     Kind kind, Integer errorCode, String providerCode, Instant decidedAt) {
    public enum Kind { SUCCESS, FAILURE }

    public SecondaryStageDecision {
        Objects.requireNonNull(decisionId);
        Objects.requireNonNull(clientMsgId);
        Objects.requireNonNull(attemptId);
        Objects.requireNonNull(kind);
        Objects.requireNonNull(decidedAt);
        if (decisionId.isBlank() || clientMsgId.isBlank() || attemptId.isBlank()
                || (kind == Kind.SUCCESS) != (errorCode == null)) {
            throw new IllegalArgumentException("Invalid secondary-stage decision");
        }
    }

    public static SecondaryStageDecision from(TcpSendResult result) {
        return new SecondaryStageDecision(result.resultId(), result.clientMsgId(), result.attemptId(),
                result.status() == TcpSendResult.Status.SUCCESS ? Kind.SUCCESS : Kind.FAILURE,
                result.errorCode(), result.providerCode(), result.observedAt());
    }
}

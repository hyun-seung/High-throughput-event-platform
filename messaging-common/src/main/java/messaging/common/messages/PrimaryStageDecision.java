package messaging.common.messages;

import java.time.Instant;
import java.util.Objects;

/** Durable result of the first-send stage, awaiting finalization or secondary handoff. */
public record PrimaryStageDecision(String decisionId, String clientMsgId, Kind kind, String source,
                                   Integer errorCode, String reason, HttpCarrier carrier,
                                   Integer invocation, boolean secondaryRequired, Instant decidedAt) {
    public enum Kind { SUCCESS, FAILURE }

    public PrimaryStageDecision {
        Objects.requireNonNull(decisionId);
        Objects.requireNonNull(clientMsgId);
        Objects.requireNonNull(kind);
        Objects.requireNonNull(source);
        Objects.requireNonNull(decidedAt);
        if (decisionId.isBlank() || clientMsgId.isBlank() || source.isBlank()
                || kind == Kind.SUCCESS && (errorCode != null || reason != null || secondaryRequired)
                || kind == Kind.FAILURE && errorCode == null && reason == null
                || (carrier == null) != (invocation == null)) {
            throw new IllegalArgumentException("Invalid primary-stage decision");
        }
    }
}

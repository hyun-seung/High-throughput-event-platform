package messaging.common.messages;

import java.util.Objects;

/** Immutable handoff to MSG-COMPLETE-MANAGER after the message is final. */
public record FinalizedMessageResult(PrimaryStageDecision decision, MessageSubmission submission) {
    public FinalizedMessageResult {
        Objects.requireNonNull(decision);
        Objects.requireNonNull(submission);
        if (decision.secondaryRequired() || !decision.clientMsgId().equals(submission.clientMsgId())) {
            throw new IllegalArgumentException("Finalized result does not match the original message");
        }
    }
}

package messaging.common.messages;

import java.util.Objects;

/** Immutable handoff after primary completion or a final second-send TCP response. */
public record FinalizedMessageResult(PrimaryStageDecision decision, SecondaryStageDecision secondaryDecision,
                                     MessageSubmission submission) {
    public FinalizedMessageResult(PrimaryStageDecision decision, MessageSubmission submission) {
        this(decision, null, submission);
    }

    public FinalizedMessageResult {
        Objects.requireNonNull(decision);
        Objects.requireNonNull(submission);
        if (!decision.clientMsgId().equals(submission.clientMsgId())
                || secondaryDecision == null && decision.secondaryRequired()
                || secondaryDecision != null && (!decision.secondaryRequired()
                || !secondaryDecision.clientMsgId().equals(submission.clientMsgId()))) {
            throw new IllegalArgumentException("Finalized result does not match the original message");
        }
    }
}

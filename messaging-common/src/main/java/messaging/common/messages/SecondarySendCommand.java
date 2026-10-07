package messaging.common.messages;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/** Immutable 2nd-send command; the TCP protocol mapping remains in MSG-TCP-SENDER. */
public record SecondarySendCommand(String attemptId, PrimaryStageDecision primaryDecision,
                                   MessageSubmission submission) {
    public SecondarySendCommand {
        Objects.requireNonNull(attemptId);
        Objects.requireNonNull(primaryDecision);
        Objects.requireNonNull(submission);
        if (attemptId.isBlank() || !primaryDecision.secondaryRequired()
                || !primaryDecision.clientMsgId().equals(submission.clientMsgId())
                || !submission.hasSecondarySendPayload()) {
            throw new IllegalArgumentException("Secondary command does not match the primary result");
        }
    }

    public static SecondarySendCommand from(PrimaryStageDecision decision, MessageSubmission submission) {
        String id = UUID.nameUUIDFromBytes(("secondary-tcp:" + decision.clientMsgId())
                .getBytes(StandardCharsets.UTF_8)).toString();
        return new SecondarySendCommand(id, decision, submission);
    }
}

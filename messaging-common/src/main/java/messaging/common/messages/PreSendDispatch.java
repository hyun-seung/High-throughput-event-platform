package messaging.common.messages;

/** Exactly one durable handoff for an admitted message. */
public record PreSendDispatch(HttpSendCommand command, PreSendFailure failure) {
    public PreSendDispatch {
        if ((command == null) == (failure == null)) {
            throw new IllegalArgumentException("Exactly one pre-send outcome is required");
        }
    }
}

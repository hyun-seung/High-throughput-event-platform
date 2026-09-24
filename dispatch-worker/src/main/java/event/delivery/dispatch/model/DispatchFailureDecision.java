package event.delivery.dispatch.model;

import java.time.Instant;
import java.util.Objects;

/** A durable next action; DECISION_PENDING is not final customer delivery status. */
public record DispatchFailureDecision(State state, String reason, Instant observedAt, Instant nextAttemptAt) {
    public enum State { RETRY_SCHEDULED, REVIEW_REQUIRED, DECISION_PENDING }

    public DispatchFailureDecision {
        Objects.requireNonNull(state);
        Objects.requireNonNull(reason);
        Objects.requireNonNull(observedAt);
        if ((state == State.RETRY_SCHEDULED) != (nextAttemptAt != null)) {
            throw new IllegalArgumentException("Only scheduled retries require nextAttemptAt");
        }
    }
}

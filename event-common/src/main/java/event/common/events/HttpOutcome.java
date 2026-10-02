package event.common.events;

import java.time.Instant;

/** One immutable observation of an HTTP provider invocation, not a final business decision. */
public record HttpOutcome(String outcomeId, String executionId, String attemptId, int invocation,
                          int version, Kind kind, Instant observedAt, Instant providerProcessedAt) {
    public enum Kind {
        ACCEPTED, RETRY_1S, RETRY_10S, FALLBACK_REQUIRED, PERMANENT_REJECTION,
        NO_RESPONSE, HTTP_ERROR, INVALID_RESPONSE, EXPIRED
    }
}

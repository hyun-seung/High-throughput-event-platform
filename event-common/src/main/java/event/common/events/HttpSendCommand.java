package event.common.events;

import java.time.Instant;
import java.util.Objects;

/** Immutable carrier-specific command; the sender selects only its configured endpoint. */
public record HttpSendCommand(String executionId, String attemptId, HttpCarrier carrier,
                              Instant deadlineAt, HttpProviderRequest request) {
    public HttpSendCommand {
        Objects.requireNonNull(executionId);
        Objects.requireNonNull(attemptId);
        Objects.requireNonNull(carrier);
        Objects.requireNonNull(deadlineAt);
        Objects.requireNonNull(request);
        if (executionId.isBlank() || attemptId.isBlank() || !executionId.equals(request.deliveryId())) {
            throw new IllegalArgumentException("HTTP send command does not match its provider body");
        }
    }
}

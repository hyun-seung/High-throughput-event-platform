package messaging.common.messages;

import java.time.Instant;
import java.util.Objects;

/** Immutable carrier-specific command; the sender selects only its configured endpoint. */
public record HttpSendCommand(String attemptId, HttpCarrier carrier, int invocation,
                              Instant deadlineAt, HttpProviderRequest request) {
    public HttpSendCommand {
        Objects.requireNonNull(attemptId);
        Objects.requireNonNull(carrier);
        Objects.requireNonNull(deadlineAt);
        Objects.requireNonNull(request);
        if (attemptId.isBlank() || invocation < 1) {
            throw new IllegalArgumentException("HTTP send command attemptId and invocation are required");
        }
    }
}

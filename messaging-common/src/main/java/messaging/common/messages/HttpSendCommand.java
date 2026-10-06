package messaging.common.messages;

import java.time.Instant;
import java.util.Objects;

/** Immutable carrier-specific command; the sender selects only its configured endpoint. */
public record HttpSendCommand(String sendRequestId, String attemptId, HttpCarrier carrier,
                              Instant deadlineAt, HttpProviderRequest request) {
    public HttpSendCommand {
        Objects.requireNonNull(sendRequestId);
        Objects.requireNonNull(attemptId);
        Objects.requireNonNull(carrier);
        Objects.requireNonNull(deadlineAt);
        Objects.requireNonNull(request);
        if (sendRequestId.isBlank() || attemptId.isBlank()
                || !ProviderClientMsgIds.forInvocation(sendRequestId, carrier, request.invocation())
                        .equals(request.clientMsgId())) {
            throw new IllegalArgumentException("HTTP send command does not match its provider body");
        }
    }
}

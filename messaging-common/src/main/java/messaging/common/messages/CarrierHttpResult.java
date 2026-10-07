package messaging.common.messages;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** One frozen observation of a carrier HTTP invocation. */
public record CarrierHttpResult(String resultId, String clientMsgId, String attemptId,
                                HttpCarrier carrier, int invocation, String source, Status status,
                                Integer httpStatus, String providerStatus, String providerErrorCode,
                                Integer normalizedErrorCode, String errorMessage, Instant observedAt) {
    public enum Status { ACCEPTED, FAILED, TIMEOUT }

    public CarrierHttpResult {
        Objects.requireNonNull(resultId);
        Objects.requireNonNull(clientMsgId);
        Objects.requireNonNull(attemptId);
        Objects.requireNonNull(carrier);
        Objects.requireNonNull(source);
        Objects.requireNonNull(status);
        Objects.requireNonNull(observedAt);
        if (resultId.isBlank() || clientMsgId.isBlank() || attemptId.isBlank() || invocation < 1) {
            throw new IllegalArgumentException("Invalid carrier HTTP result identity");
        }
        if (status == Status.ACCEPTED && (!"HTTP_RESPONSE".equals(source) || !Integer.valueOf(200).equals(httpStatus)
                || normalizedErrorCode != null)) {
            throw new IllegalArgumentException("Invalid carrier HTTP acceptance");
        }
        if (status == Status.FAILED && (!"HTTP_RESPONSE".equals(source) || httpStatus == null
                || normalizedErrorCode == null)) {
            throw new IllegalArgumentException("Invalid carrier HTTP failure");
        }
        if (status == Status.TIMEOUT && (!"HTTP_TIMEOUT".equals(source) || httpStatus != null
                || normalizedErrorCode != null)) {
            throw new IllegalArgumentException("Invalid carrier HTTP timeout");
        }
    }

    public static String id(HttpSendCommand command) {
        return id(command.request().clientMsgId(), command.attemptId(), command.invocation());
    }

    public static String id(String clientMsgId, String attemptId, int invocation) {
        return UUID.nameUUIDFromBytes(("carrier-http-result:" + clientMsgId + ":"
                + attemptId + ":" + invocation).getBytes(StandardCharsets.UTF_8)).toString();
    }

    public boolean needsPublication() { return status != Status.ACCEPTED; }
}

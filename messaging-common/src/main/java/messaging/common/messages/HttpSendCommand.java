package messaging.common.messages;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

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

    public static String attemptId(String clientMsgId, HttpCarrier carrier) {
        Objects.requireNonNull(clientMsgId);
        Objects.requireNonNull(carrier);
        return UUID.nameUUIDFromBytes(("primary-http:" + clientMsgId + ":" + carrier.name())
                .getBytes(StandardCharsets.UTF_8)).toString();
    }

    public Map<String, AttributeValue> stepKey() {
        return Map.of("pk", AttributeValue.fromS("DELIVERY#" + request.clientMsgId()),
                "sk", AttributeValue.fromS("HTTP#" + carrier.name() + "#" + attemptId + "#" + invocation));
    }
}

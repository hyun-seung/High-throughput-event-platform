package messaging.common.messages;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** One result decision authorizes exactly one later carrier HTTP command. */
public record FollowupHttpCommand(String decisionId, HttpSendCommand command, Instant notBefore) {
    public static final String CURRENT_DECISION = "result_decision_id";
    public static final String CURRENT_COMMAND = "result_current_http_command";

    public FollowupHttpCommand {
        Objects.requireNonNull(decisionId);
        Objects.requireNonNull(command);
        Objects.requireNonNull(notBefore);
        if (decisionId.isBlank()) {
            throw new IllegalArgumentException("Invalid follow-up HTTP command authorization");
        }
    }

    public static Map<String, AttributeValue> key(HttpSendCommand command) {
        return Map.of("pk", AttributeValue.fromS("DELIVERY#" + command.request().clientMsgId()),
                "sk", AttributeValue.fromS("HTTP_COMMAND#" + command.carrier().name() + "#"
                        + command.attemptId() + "#" + command.invocation()));
    }
}

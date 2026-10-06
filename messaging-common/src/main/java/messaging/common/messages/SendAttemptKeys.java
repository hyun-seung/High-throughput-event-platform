package messaging.common.messages;

import java.util.Objects;

/** Redis claims identify a specific provider call, not the stable message ID alone. */
public final class SendAttemptKeys {
    private SendAttemptKeys() { }

    public static String http(HttpSendCommand command) {
        Objects.requireNonNull(command);
        return "message:http:attempt:" + command.request().clientMsgId() + ":" + command.carrier().name()
                + ":" + command.attemptId() + ":" + command.invocation();
    }

    public static String tcp(String clientMsgId, String attemptId) {
        if (clientMsgId == null || clientMsgId.isBlank() || attemptId == null || attemptId.isBlank()) {
            throw new IllegalArgumentException("TCP clientMsgId and attemptId are required");
        }
        return "message:tcp:attempt:" + clientMsgId + ":" + attemptId;
    }
}

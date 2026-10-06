package messaging.common.messages;

import java.nio.charset.StandardCharsets;

/** One provider result; the provider echoes only the stable clientMsgId. */
public record MessageWebhookResult(String clientMsgId, String status, Error error) {
    public MessageWebhookResult {
        if (clientMsgId == null || clientMsgId.isBlank()
                || clientMsgId.getBytes(StandardCharsets.UTF_8).length > 40) {
            throw new IllegalArgumentException("Invalid clientMsgId");
        }
        if ("success".equals(status)) {
            if (error != null) throw new IllegalArgumentException("Success must not include error");
        } else if ("fail".equals(status)) {
            if (error == null) throw new IllegalArgumentException("Failure requires error");
        } else {
            throw new IllegalArgumentException("Invalid webhook status");
        }
    }

    public record Error(int code, String message) {
        public Error {
            if (code < 10000 || code > 99999 || message == null || message.isBlank()) {
                throw new IllegalArgumentException("Invalid webhook error");
            }
        }
    }
}

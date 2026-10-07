package messaging.common.messages;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/** Stable per-message handoff to the customer webhook sender. */
public record CustomerWebhookSendCommand(String webhookId, FinalizedMessageResult finalized) {
    public CustomerWebhookSendCommand {
        Objects.requireNonNull(webhookId);
        Objects.requireNonNull(finalized);
        if (!webhookId.equals(webhookId(finalized.decision().clientMsgId()))) {
            throw new IllegalArgumentException("Customer webhook ID does not match the finalized message");
        }
    }

    public static CustomerWebhookSendCommand from(FinalizedMessageResult finalized) {
        return new CustomerWebhookSendCommand(webhookId(finalized.decision().clientMsgId()), finalized);
    }

    private static String webhookId(String clientMsgId) {
        return UUID.nameUUIDFromBytes(("customer-webhook:" + clientMsgId)
                .getBytes(StandardCharsets.UTF_8)).toString();
    }
}

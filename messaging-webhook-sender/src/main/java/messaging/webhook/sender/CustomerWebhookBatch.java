package messaging.webhook.sender;

import messaging.common.messages.CustomerWebhookSendCommand;
import messaging.common.messages.PrimaryStageDecision;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Stable customer-facing body; exactly one client and at most 100 final results. */
public record CustomerWebhookBatch(int schemaVersion, UUID batchId, long clientId, List<Result> results) {
    public CustomerWebhookBatch {
        results = List.copyOf(results);
        if (schemaVersion != 1 || clientId <= 0 || results.isEmpty() || results.size() > 100) {
            throw new IllegalArgumentException("Invalid customer webhook batch");
        }
    }

    public record Result(UUID webhookId, String clientMsgId, String messageId, String status,
                         Integer errorCode, String reason, Instant decidedAt) {
        public static Result from(CustomerWebhookSendCommand command) {
            var decision = command.finalized().decision();
            return new Result(UUID.fromString(command.webhookId()), decision.clientMsgId(),
                    command.finalized().submission().messageId(),
                    decision.kind() == PrimaryStageDecision.Kind.SUCCESS ? "success" : "fail",
                    decision.errorCode(), decision.reason(), decision.decidedAt());
        }
    }
}

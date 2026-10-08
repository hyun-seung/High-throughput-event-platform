package messaging.webhook.receive;

import messaging.common.messages.MessageWebhookBatch;

import java.util.concurrent.CompletableFuture;

public interface MessageWebhookPublisher {
    CompletableFuture<Void> publish(MessageWebhookBatch batch);
}

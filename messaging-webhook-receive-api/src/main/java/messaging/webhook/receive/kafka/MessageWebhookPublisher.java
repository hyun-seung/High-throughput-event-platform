package messaging.webhook.receive.kafka;

import messaging.common.messages.MessageWebhookBatch;

import java.util.concurrent.CompletableFuture;

public interface MessageWebhookPublisher {
    CompletableFuture<Void> publish(MessageWebhookBatch batch);
}

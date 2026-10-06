package messaging.webhook.receive.kafka;

import messaging.common.messages.MessageTopics;
import messaging.common.messages.MessageWebhookBatch;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class KafkaMessageWebhookPublisher implements MessageWebhookPublisher {
    private final KafkaTemplate<String, MessageWebhookBatch> kafka;

    public KafkaMessageWebhookPublisher(KafkaTemplate<String, MessageWebhookBatch> kafka) { this.kafka = kafka; }

    @Override
    public CompletableFuture<Void> publish(MessageWebhookBatch batch) {
        return kafka.send(MessageTopics.MSG_RESULT, batch.traceId(), batch).thenApply(ignored -> null);
    }
}

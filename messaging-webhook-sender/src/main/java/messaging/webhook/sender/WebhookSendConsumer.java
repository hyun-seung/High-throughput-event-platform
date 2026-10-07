package messaging.webhook.sender;

import messaging.common.messages.CustomerWebhookSendCommand;
import messaging.common.messages.MessageTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.Objects;

@Component
@ConditionalOnProperty(prefix = "messaging.webhook.sender", name = "enabled", havingValue = "true")
public class WebhookSendConsumer {
    private final WebhookBatchRepository repository;
    private final JsonMapper mapper;

    public WebhookSendConsumer(WebhookBatchRepository repository, JsonMapper mapper) {
        this.repository = Objects.requireNonNull(repository);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @KafkaListener(topics = MessageTopics.WEBHOOK_SEND, groupId = "messaging-webhook-sender")
    public void receive(ConsumerRecord<String, String> record) {
        if (record.key() == null || record.value() == null) throw new IllegalArgumentException("Webhook command needs key and value");
        var command = mapper.readValue(record.value(), CustomerWebhookSendCommand.class);
        if (!record.key().equals(command.finalized().decision().clientMsgId())) {
            throw new IllegalArgumentException("Webhook Kafka key differs from clientMsgId");
        }
        repository.capture(command);
    }
}

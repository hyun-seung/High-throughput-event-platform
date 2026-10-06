package messaging.api.messages;

import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageTopics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class KafkaMessageRequestPublisher implements MessageRequestPublisher {
    private final KafkaTemplate<String, MessageSubmission> kafka;

    public KafkaMessageRequestPublisher(KafkaTemplate<String, MessageSubmission> kafka) {
        this.kafka = kafka;
    }

    @Override
    public CompletableFuture<?> publish(MessageSubmission event) {
        return kafka.send(MessageTopics.RECEIVED, event.clientMsgId(), event);
    }
}

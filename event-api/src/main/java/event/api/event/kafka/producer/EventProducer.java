package event.api.event.kafka.producer;

import event.common.message.EventMessage;
import event.common.topic.EventTopics;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
public class EventProducer {

    private final KafkaTemplate<String, EventMessage> kafkaTemplate;

    public CompletableFuture<SendResult<String, EventMessage>> send(EventMessage message) {
        return kafkaTemplate.send(EventTopics.EVENT_REQUESTS, message.eventId(), message);
    }
}

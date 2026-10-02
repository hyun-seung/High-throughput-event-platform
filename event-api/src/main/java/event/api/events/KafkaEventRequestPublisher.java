package event.api.events;

import event.common.events.EventSubmission;
import event.common.events.EventTopics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class KafkaEventRequestPublisher implements EventRequestPublisher {
    private final KafkaTemplate<String, EventSubmission> kafka;

    public KafkaEventRequestPublisher(KafkaTemplate<String, EventSubmission> kafka) {
        this.kafka = kafka;
    }

    @Override
    public CompletableFuture<?> publish(EventSubmission event) {
        return kafka.send(EventTopics.HTTP_REQUESTED, event.executionId(), event);
    }
}

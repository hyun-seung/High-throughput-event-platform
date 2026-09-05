package event.processing.worker.event.consumer;

import event.contract.message.EventMessage;
import event.contract.topic.EventTopics;
import event.processing.worker.event.service.EventProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventRequestConsumer {

    private final EventProcessingService eventProcessingService;

    @KafkaListener(topics = EventTopics.EVENT_REQUESTS)
    public void consume(EventMessage message) {
        log.debug("Event consumed. eventId={}, userId={}, eventType={}, payload={}, occurredAt={}",
                message.eventId(), message.userId(), message.eventType(), message.payload(), message.occurredAt());

        eventProcessingService.process(message);
    }
}
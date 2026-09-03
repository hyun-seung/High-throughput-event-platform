package event.event.api.event.kafka.producer;

import event.event.api.event.dto.EventMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventProducer {

    private static final String EVENT_REQUEST_TOPIC = "event.requests";

    private final KafkaTemplate<String, EventMessage> kafkaTemplate;

    public void send(EventMessage message) {
        kafkaTemplate.send(EVENT_REQUEST_TOPIC, message.eventId(), message)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Kafka event produce failed. eventId={}, cause={}", message.eventId(), exception.getMessage());
                        return;
                    }

                    log.debug("Kafka event produced. eventId={}, topic={}, partition={}, offset={}",
                            message.eventId(),
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset()
                    );
                });
    }
}
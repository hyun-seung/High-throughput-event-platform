package event.reference.cache;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ReferenceCdcConsumer {
    private final ReferenceCdcProjector projector;

    public ReferenceCdcConsumer(ReferenceCdcProjector projector) {
        this.projector = projector;
    }

    @KafkaListener(topics = {ReferenceCdcTopics.CONTRACTS, ReferenceCdcTopics.PHONE_CARRIERS},
            groupId = "event-reference-cache")
    public void receive(ConsumerRecord<String, String> record) {
        projector.apply(record.topic(), record.key(), record.value());
    }
}

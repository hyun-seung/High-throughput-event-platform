package event.api.delivery.kafka;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryTopics;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
public class DeliveryProducer implements DeliveryEventPublisher {

    private final KafkaTemplate<String, DeliveryEvent> kafkaTemplate;

    @Override
    public CompletableFuture<SendResult<String, DeliveryEvent>> send(DeliveryEvent event) {
        return kafkaTemplate.send(DeliveryTopics.DELIVERY_REQUESTED, event.deliveryId(), event);
    }
}

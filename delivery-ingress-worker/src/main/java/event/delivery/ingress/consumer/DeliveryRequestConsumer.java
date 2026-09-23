package event.delivery.ingress.consumer;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryEventType;
import event.common.delivery.DeliveryTopics;
import event.delivery.ingress.repository.DeliveryRepository;
import event.delivery.ingress.service.DeliveryFlowProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeliveryRequestConsumer {

    private final DeliveryRepository deliveryRepository;
    private final DeliveryFlowProducer deliveryFlowProducer;

    @KafkaListener(
            topics = DeliveryTopics.DELIVERY_REQUESTED,
            groupId = "delivery-ingress-worker"
    )
    public void consume(DeliveryEvent event) {
        requireType(event, DeliveryEventType.DELIVERY_REQUESTED);

        DeliveryEvent stored = deliveryRepository.saveOrLoad(event);
        log.debug("Delivery request consumed. deliveryId={}, eventId={}, occurredAt={}",
                stored.deliveryId(), stored.eventId(), stored.occurredAt());

        deliveryFlowProducer.sendDispatchRequested(stored.toDispatchRequested()).join();
    }

    private void requireType(DeliveryEvent event, DeliveryEventType expected) {
        if (event.eventType() != expected) {
            throw new IllegalArgumentException(
                    "Unexpected delivery event type. expected=" + expected + ", actual=" + event.eventType());
        }
    }
}

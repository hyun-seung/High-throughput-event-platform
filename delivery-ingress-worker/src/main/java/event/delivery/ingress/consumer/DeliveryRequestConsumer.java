package event.delivery.ingress.consumer;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryEventType;
import event.common.delivery.DeliveryTopics;
import event.common.metrics.DeliveryMetrics;
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
    private final DeliveryMetrics metrics;

    @KafkaListener(
            topics = DeliveryTopics.DELIVERY_REQUESTED,
            groupId = "delivery-ingress-worker"
    )
    public void consume(DeliveryEvent event) {
        metrics.measure(DeliveryMetrics.Stage.INGRESS_PROCESS, () -> process(event));
    }

    private void process(DeliveryEvent event) {
        requireType(event, DeliveryEventType.DELIVERY_REQUESTED);

        DeliveryEvent stored = metrics.measure(DeliveryMetrics.Stage.INGRESS_STORE,
                () -> deliveryRepository.saveOrLoad(event));
        log.debug("Delivery request consumed. deliveryId={}, eventId={}, occurredAt={}",
                stored.deliveryId(), stored.eventId(), stored.occurredAt());

        metrics.measure(DeliveryMetrics.Stage.INGRESS_PUBLISH,
                () -> deliveryFlowProducer.sendDispatchRequested(stored.toDispatchRequested()).join());
        metrics.outcome(DeliveryMetrics.Outcome.INGRESS_FORWARDED);
    }

    private void requireType(DeliveryEvent event, DeliveryEventType expected) {
        if (event.eventType() != expected) {
            throw new IllegalArgumentException(
                    "Unexpected delivery event type. expected=" + expected + ", actual=" + event.eventType());
        }
    }
}

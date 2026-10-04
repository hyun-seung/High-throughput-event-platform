package messaging.delivery.ingress.consumer;

import messaging.common.delivery.DeliveryEvent;
import messaging.common.delivery.DeliveryEventType;
import messaging.common.delivery.DeliveryTopics;
import messaging.common.metrics.DeliveryMetrics;
import messaging.common.metrics.DeliveryAudit;
import messaging.delivery.ingress.repository.DeliveryRepository;
import messaging.delivery.ingress.service.DeliveryFlowProducer;
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

        var saved = metrics.measure(DeliveryMetrics.Stage.INGRESS_STORE,
                () -> deliveryRepository.saveOrLoad(event));
        if (saved.completed()) {
            DeliveryAudit.record(event, "ingress", "completed_duplicate", "", "", "preserved");
            return;
        }
        DeliveryEvent stored = saved.event();
        log.debug("Delivery request consumed. deliveryId={}, eventId={}, occurredAt={}",
                stored.deliveryId(), stored.eventId(), stored.occurredAt());

        metrics.measure(DeliveryMetrics.Stage.INGRESS_PUBLISH,
                () -> deliveryFlowProducer.sendDispatchRequested(stored.toDispatchRequested()).join());
        metrics.outcome(DeliveryMetrics.Outcome.INGRESS_FORWARDED);
        DeliveryAudit.record(stored, "ingress", "forwarded", "", "", "acknowledged");
    }

    private void requireType(DeliveryEvent event, DeliveryEventType expected) {
        if (event.eventType() != expected) {
            throw new IllegalArgumentException(
                    "Unexpected delivery event type. expected=" + expected + ", actual=" + event.eventType());
        }
    }
}

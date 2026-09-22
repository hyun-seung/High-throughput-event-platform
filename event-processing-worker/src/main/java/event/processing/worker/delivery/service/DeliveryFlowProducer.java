package event.processing.worker.delivery.service;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryTopics;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
public class DeliveryFlowProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public CompletableFuture<SendResult<String, Object>> sendDispatchRequested(DeliveryEvent event) {
        return kafkaTemplate.send(DeliveryTopics.DISPATCH_REQUESTED, event.deliveryId(), event);
    }
}

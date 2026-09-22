package event.delivery.dispatch.consumer;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryEventType;
import event.common.delivery.DeliveryTopics;
import event.delivery.dispatch.external.client.ExternalApiClient;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DispatchRequestConsumer {

    private final ExternalApiClient externalApiClient;

    @KafkaListener(
            topics = DeliveryTopics.DISPATCH_REQUESTED,
            groupId = "delivery-dispatch-worker"
    )
    public void consume(DeliveryEvent event) {
        if (event.eventType() != DeliveryEventType.DISPATCH_REQUESTED) {
            throw new IllegalArgumentException("Unexpected delivery event type: " + event.eventType());
        }

        ProviderDispatchResponse response = externalApiClient.send(event);

        log.debug("Dispatch completed. deliveryId={}, providerAccepted={}, processedAt={}",
                event.deliveryId(), response.accepted(), response.processedAt());
    }
}

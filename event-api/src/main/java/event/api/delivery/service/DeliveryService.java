package event.api.delivery.service;

import event.api.delivery.dto.DeliveryRequest;
import event.api.delivery.dto.DeliveryResponse;
import event.api.delivery.kafka.DeliveryEventPublisher;
import event.api.security.principal.AuthenticatedUser;
import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryIds;
import event.common.metrics.DeliveryMetrics;
import event.common.metrics.DeliveryAudit;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class DeliveryService {

    private static final String ACCEPTED = "ACCEPTED";

    private final DeliveryEventPublisher deliveryEventPublisher;
    private final DeliveryMetrics metrics;

    public CompletableFuture<DeliveryResponse> accept(
            AuthenticatedUser user,
            String idempotencyKey,
            DeliveryRequest request
    ) {
        String deliveryId = DeliveryIds.deliveryId(user.userId(), idempotencyKey);
        DeliveryEvent event = DeliveryEvent.requested(
                deliveryId,
                user.userId(),
                request.deliveryType(),
                request.payload(),
                Instant.now()
        );

        try {
            return metrics.measureAsync(DeliveryMetrics.Stage.API_PUBLISH, () -> deliveryEventPublisher.send(event))
                    .handle((ignored, failure) -> {
                        if (failure != null) {
                            DeliveryAudit.record(event, "api", "publish_unconfirmed", "", "", "kafka_ack_failed");
                            throw new DeliveryAcceptanceException(failure);
                        }
                        DeliveryAudit.record(event, "api", "accepted", "", "", "202");
                        return new DeliveryResponse(deliveryId, ACCEPTED);
                    });
        } catch (RuntimeException failure) {
            DeliveryAudit.record(event, "api", "publish_unconfirmed", "", "", "publish_failed");
            // Kafka send can fail before returning a future (metadata/buffer/serialization).
            return CompletableFuture.failedFuture(new DeliveryAcceptanceException(failure));
        }
    }
}

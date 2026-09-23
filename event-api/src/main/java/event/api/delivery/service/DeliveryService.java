package event.api.delivery.service;

import event.api.delivery.dto.DeliveryRequest;
import event.api.delivery.dto.DeliveryResponse;
import event.api.delivery.kafka.DeliveryEventPublisher;
import event.api.security.principal.AuthenticatedUser;
import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryIds;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class DeliveryService {

    private static final String ACCEPTED = "ACCEPTED";

    private final DeliveryEventPublisher deliveryEventPublisher;

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
            return deliveryEventPublisher.send(event)
                    .handle((ignored, failure) -> {
                        if (failure != null) {
                            throw new DeliveryAcceptanceException(failure);
                        }
                        return new DeliveryResponse(deliveryId, ACCEPTED);
                    });
        } catch (RuntimeException failure) {
            // Kafka send can fail before returning a future (metadata/buffer/serialization).
            return CompletableFuture.failedFuture(new DeliveryAcceptanceException(failure));
        }
    }
}

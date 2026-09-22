package event.delivery.dispatch.external.dto;

import java.time.Instant;

public record ProviderDispatchResponse(
        String deliveryId,
        boolean accepted,
        Instant processedAt
) {
}

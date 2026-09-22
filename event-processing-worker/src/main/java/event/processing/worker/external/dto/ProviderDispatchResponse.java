package event.processing.worker.external.dto;

import java.time.Instant;

public record ProviderDispatchResponse(
        String deliveryId,
        boolean accepted,
        Instant processedAt
) {
}

package external.api.simulator.delivery.dto;

import java.time.Instant;

public record ProviderDispatchResponse(
        String deliveryId,
        boolean accepted,
        Instant processedAt
) {
}

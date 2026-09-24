package event.delivery.dispatch.external.dto;

import java.time.Instant;

public record ProviderDispatchResponse(
        String deliveryId,
        Boolean accepted,
        Instant processedAt,
        String code
) {
    public ProviderDispatchResponse(String deliveryId, boolean accepted, Instant processedAt) {
        this(deliveryId, accepted, processedAt, accepted ? "ACCEPTED" : null);
    }
}

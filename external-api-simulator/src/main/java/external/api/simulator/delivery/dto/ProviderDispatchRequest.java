package external.api.simulator.delivery.dto;

import java.time.Instant;
import java.util.Map;

public record ProviderDispatchRequest(
        String deliveryId,
        Long tenantId,
        String deliveryType,
        Map<String, Object> payload,
        Instant occurredAt
) {
}

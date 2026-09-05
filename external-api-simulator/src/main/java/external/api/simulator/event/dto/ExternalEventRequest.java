package external.api.simulator.event.dto;

import java.time.Instant;
import java.util.Map;

public record ExternalEventRequest(
        String eventId,
        Long userId,
        String eventType,
        Map<String, Object> payload,
        Instant occurredAt
) {
}
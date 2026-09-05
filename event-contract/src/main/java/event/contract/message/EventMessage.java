package event.contract.message;

import java.time.Instant;
import java.util.Map;

public record EventMessage(
        String eventId,
        Long userId,
        String eventType,
        Map<String, Object> payload,
        Instant occurredAt
) {
}
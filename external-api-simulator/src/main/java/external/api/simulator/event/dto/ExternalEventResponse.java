package external.api.simulator.event.dto;

import java.time.Instant;

public record ExternalEventResponse(
        String eventId,
        boolean success,
        Instant processedAt
) {
}
package event.processing.worker.external.dto;

import java.time.Instant;

public record ExternalEventResponse(
        String eventId,
        boolean success,
        Instant processedAt
) {
}
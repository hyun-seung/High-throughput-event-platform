package event.processing.worker.external.dto;

import event.contract.message.EventMessage;

import java.time.Instant;
import java.util.Map;

public record ExternalEventRequest(
        String eventId,
        Long userId,
        String eventType,
        Map<String, Object> payload,
        Instant occurredAt
) {

    public static ExternalEventRequest from(EventMessage message) {
        return new ExternalEventRequest(
                message.eventId(),
                message.userId(),
                message.eventType(),
                message.payload(),
                message.occurredAt()
        );
    }
}
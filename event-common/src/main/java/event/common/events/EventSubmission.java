package event.common.events;

import event.common.delivery.DeliveryPayloads;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Immutable customer input; executionId is generated once by EVENT-RECEIVE-API. */
public record EventSubmission(
        String executionId,
        long clientId,
        String eventId,
        String recipientNumber,
        EventType eventType,
        Map<String, Object> payload,
        boolean fallbackAllowed,
        Instant receivedAt
) {
    public EventSubmission {
        Objects.requireNonNull(executionId);
        Objects.requireNonNull(eventId);
        Objects.requireNonNull(recipientNumber);
        Objects.requireNonNull(eventType);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(receivedAt);
        payload = DeliveryPayloads.canonicalize(payload);
    }
}

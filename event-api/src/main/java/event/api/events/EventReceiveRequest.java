package event.api.events;

import event.common.events.EventType;

import java.util.Map;

/** Customer input validated before durable admission. */
public record EventReceiveRequest(String eventId, String recipientNumber, EventType eventType,
                                  Map<String, Object> payload, Boolean fallbackAllowed) {
    public boolean allowFallback() { return Boolean.TRUE.equals(fallbackAllowed); }
}

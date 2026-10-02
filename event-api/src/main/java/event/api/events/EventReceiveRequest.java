package event.api.events;

import event.common.events.EventType;

import java.util.Map;

/** Parsed before contract charging; business validation runs afterwards. */
public record EventReceiveRequest(String eventId, String recipientNumber, EventType eventType,
                                  Map<String, Object> payload, Boolean fallbackAllowed) {
    public boolean allowFallback() { return Boolean.TRUE.equals(fallbackAllowed); }
}

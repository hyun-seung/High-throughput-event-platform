package messaging.api.messages;

import messaging.common.messages.MessageCategory;

import java.util.Map;

/** Customer input validated before durable admission. */
public record MessageReceiveRequest(String messageId, String recipientNumber, MessageCategory messageCategory,
                                  Map<String, Object> payload, Boolean fallbackAllowed) {
    public boolean allowFallback() { return Boolean.TRUE.equals(fallbackAllowed); }
}

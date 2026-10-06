package messaging.common.messages;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** The immutable admission fields of an ORIGIN item. */
public final class MessageOriginCodec {
    public static final String STATUS_RECEIVED = "RECEIVED";
    public static final String MESSAGE_ID = "message_id";
    public static final String RECIPIENT_NUMBER = "recipient_number";

    private MessageOriginCodec() { }

    public static Map<String, AttributeValue> key(String clientMsgId) {
        return Map.of("pk", AttributeValue.fromS("DELIVERY#" + clientMsgId),
                "sk", AttributeValue.fromS("META"));
    }

    public static Map<String, AttributeValue> encode(MessageSubmission event, JsonMapper mapper) {
        Map<String, AttributeValue> item = new HashMap<>(key(event.clientMsgId()));
        item.put("schema_version", AttributeValue.fromN("3"));
        // Existing DynamoDB key attributes keep their physical names; both values are clientMsgId.
        item.put("delivery_id", AttributeValue.fromS(event.clientMsgId()));
        item.put("request_key", AttributeValue.fromS(event.clientMsgId()));
        item.put("tenant_id", AttributeValue.fromN(Long.toString(event.clientId())));
        item.put(MESSAGE_ID, AttributeValue.fromS(event.messageId()));
        item.put(RECIPIENT_NUMBER, AttributeValue.fromS(event.recipientNumber()));
        item.put("delivery_type", AttributeValue.fromS(event.messageCategory().name()));
        item.put("event_type", AttributeValue.fromS("MESSAGE_RECEIVED"));
        item.put("payload", AttributeValue.fromS(mapper.writeValueAsString(event.payload())));
        item.put("fallback_allowed", AttributeValue.fromBool(event.fallbackAllowed()));
        item.put("occurred_at", AttributeValue.fromS(event.receivedAt().toString()));
        item.put("created_at", AttributeValue.fromS(event.receivedAt().toString()));
        item.put("updated_at", AttributeValue.fromS(event.receivedAt().toString()));
        item.put("status", AttributeValue.fromS(STATUS_RECEIVED));
        MessagePublicationIndex.add(item, event);
        return item;
    }

    @SuppressWarnings("unchecked")
    public static MessageSubmission decode(Map<String, AttributeValue> item, JsonMapper mapper) {
        if (item == null || item.isEmpty() || !item.containsKey(MESSAGE_ID)) {
            throw new IllegalArgumentException("Not a v3 message ORIGIN");
        }
        Map<String, Object> payload = mapper.readValue(item.get("payload").s(), Map.class);
        return new MessageSubmission(item.get("delivery_id").s(),
                Long.parseLong(item.get("tenant_id").n()),
                item.get(MESSAGE_ID).s(), item.get(RECIPIENT_NUMBER).s(),
                MessageCategory.valueOf(item.get("delivery_type").s()), payload,
                item.get("fallback_allowed").bool(), Instant.parse(item.get("occurred_at").s()));
    }
}

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
        item.put("schema_version", AttributeValue.fromN("4"));
        // Existing DynamoDB key attributes keep their physical names; both values are clientMsgId.
        item.put("delivery_id", AttributeValue.fromS(event.clientMsgId()));
        item.put("request_key", AttributeValue.fromS(event.clientMsgId()));
        item.put("tenant_id", AttributeValue.fromN(Long.toString(event.clientId())));
        item.put(MESSAGE_ID, AttributeValue.fromS(event.messageId()));
        item.put(RECIPIENT_NUMBER, AttributeValue.fromS(event.recipientNumber()));
        item.put("delivery_type", AttributeValue.fromS(event.messageCategory().name()));
        item.put("event_type", AttributeValue.fromS("MESSAGE_RECEIVED"));
        item.put("payload", AttributeValue.fromS(mapper.writeValueAsString(event.payload())));
        if (event.hasSecondarySendPayload()) {
            item.put("secondary_send_payload", AttributeValue.fromS(mapper.writeValueAsString(event.secondarySendPayload())));
        }
        item.put("occurred_at", AttributeValue.fromS(event.receivedAt().toString()));
        item.put("created_at", AttributeValue.fromS(event.receivedAt().toString()));
        item.put("updated_at", AttributeValue.fromS(event.receivedAt().toString()));
        item.put("status", AttributeValue.fromS(STATUS_RECEIVED));
        MessagePublicationIndex.add(item, event);
        return item;
    }

    @SuppressWarnings("unchecked")
    public static MessageSubmission decode(Map<String, AttributeValue> item, JsonMapper mapper) {
        if (item == null || item.isEmpty() || !item.containsKey(MESSAGE_ID)
                || !"4".equals(item.getOrDefault("schema_version", AttributeValue.fromN("0")).n())) {
            throw new IllegalArgumentException("Not a v4 message ORIGIN");
        }
        Map<String, Object> payload = mapper.readValue(item.get("payload").s(), Map.class);
        Map<String, Object> secondarySendPayload = item.containsKey("secondary_send_payload")
                ? mapper.readValue(item.get("secondary_send_payload").s(), Map.class) : null;
        return new MessageSubmission(item.get("delivery_id").s(),
                Long.parseLong(item.get("tenant_id").n()),
                item.get(MESSAGE_ID).s(), item.get(RECIPIENT_NUMBER).s(),
                MessageCategory.valueOf(item.get("delivery_type").s()), payload,
                secondarySendPayload, Instant.parse(item.get("occurred_at").s()));
    }
}

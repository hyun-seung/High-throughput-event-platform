package event.common.events;

import event.common.lifecycle.LifecycleIndex;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** The immutable admission fields of an ORIGIN item. */
public final class EventOriginCodec {
    public static final String STATUS_RECEIVED = "RECEIVED";
    public static final String CLIENT_EVENT_ID = "client_event_id";
    public static final String RECIPIENT_NUMBER = "recipient_number";

    private EventOriginCodec() { }

    public static Map<String, AttributeValue> key(String executionId) {
        return Map.of("pk", AttributeValue.fromS("DELIVERY#" + executionId),
                "sk", AttributeValue.fromS("META"));
    }

    public static Map<String, AttributeValue> encode(EventSubmission event, JsonMapper mapper) {
        Map<String, AttributeValue> item = new HashMap<>(key(event.executionId()));
        item.put("schema_version", AttributeValue.fromN("3"));
        item.put("delivery_id", AttributeValue.fromS(event.executionId()));
        item.put("request_key", AttributeValue.fromS(event.executionId()));
        item.put("tenant_id", AttributeValue.fromN(Long.toString(event.clientId())));
        item.put(CLIENT_EVENT_ID, AttributeValue.fromS(event.eventId()));
        item.put(RECIPIENT_NUMBER, AttributeValue.fromS(event.recipientNumber()));
        item.put("delivery_type", AttributeValue.fromS(event.eventType().name()));
        item.put("event_type", AttributeValue.fromS("EVENT_REQUESTED"));
        item.put("payload", AttributeValue.fromS(mapper.writeValueAsString(event.payload())));
        item.put("fallback_allowed", AttributeValue.fromBool(event.fallbackAllowed()));
        item.put("occurred_at", AttributeValue.fromS(event.receivedAt().toString()));
        item.put("created_at", AttributeValue.fromS(event.receivedAt().toString()));
        item.put("updated_at", AttributeValue.fromS(event.receivedAt().toString()));
        item.put("status", AttributeValue.fromS(STATUS_RECEIVED));
        LifecycleIndex.add(item, event.executionId(), 0);
        return item;
    }

    @SuppressWarnings("unchecked")
    public static EventSubmission decode(Map<String, AttributeValue> item, JsonMapper mapper) {
        if (item == null || item.isEmpty() || !item.containsKey(CLIENT_EVENT_ID)) {
            throw new IllegalArgumentException("Not a v3 event ORIGIN");
        }
        Map<String, Object> payload = mapper.readValue(item.get("payload").s(), Map.class);
        return new EventSubmission(item.get("delivery_id").s(),
                Long.parseLong(item.get("tenant_id").n()),
                item.get(CLIENT_EVENT_ID).s(), item.get(RECIPIENT_NUMBER).s(),
                EventType.valueOf(item.get("delivery_type").s()), payload,
                item.get("fallback_allowed").bool(), Instant.parse(item.get("occurred_at").s()));
    }
}

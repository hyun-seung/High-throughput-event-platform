package messaging.result;

import messaging.common.messages.CarrierHttpResult;
import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageResultInboxIndex;
import messaging.common.messages.MessageWebhookResult;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

/** Durably splits a mixed MSG_RESULT record into independently decidable message items. */
public class MessageResultInboxStore {
    public record Item(String clientMsgId, String resultId, String source, String payload, Instant receivedAt) {
        public Item {
            Objects.requireNonNull(clientMsgId);
            Objects.requireNonNull(resultId);
            Objects.requireNonNull(source);
            Objects.requireNonNull(payload);
            Objects.requireNonNull(receivedAt);
        }

        public Map<String, AttributeValue> key() {
            return Map.of("pk", s("DELIVERY#" + clientMsgId), "sk", s("RESULT_INBOX#" + resultId));
        }
    }

    public record WebhookItem(String traceId, HttpCarrier carrier, Instant receivedAt,
                              MessageWebhookResult result) { }

    private final DynamoDbClient db;
    private final JsonMapper mapper;
    private final Clock clock;

    public MessageResultInboxStore(DynamoDbClient db, JsonMapper mapper, Clock clock) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
        this.clock = Objects.requireNonNull(clock);
    }

    public List<Item> capture(MessageResultInput input) {
        Objects.requireNonNull(input);
        var items = new ArrayList<Item>();
        if (input instanceof MessageResultInput.Http http) {
            CarrierHttpResult result = http.result();
            items.add(new Item(result.clientMsgId(), result.resultId(), result.source(),
                    mapper.writeValueAsString(result), result.observedAt()));
        } else if (input instanceof MessageResultInput.PreSend preSend) {
            var failure = preSend.failure();
            items.add(new Item(failure.clientMsgId(), failure.resultId(), failure.source(),
                    mapper.writeValueAsString(failure), failure.observedAt()));
        } else if (input instanceof MessageResultInput.Tcp tcp) {
            var result = tcp.result();
            items.add(new Item(result.clientMsgId(), result.resultId(), result.source(),
                    mapper.writeValueAsString(result), result.observedAt()));
        } else if (input instanceof MessageResultInput.Webhook webhook) {
            var batch = webhook.batch();
            for (MessageWebhookResult result : batch.results()) {
                String resultId = webhookResultId(batch.traceId(), result.clientMsgId());
                items.add(new Item(result.clientMsgId(), resultId, batch.source(),
                        mapper.writeValueAsString(new WebhookItem(batch.traceId(), batch.carrier(),
                                batch.receivedAt(), result)), batch.receivedAt()));
            }
        } else {
            throw new IllegalArgumentException("Unsupported MSG_RESULT input");
        }
        for (Item item : items) put(item);
        return List.copyOf(items);
    }

    public static String webhookResultId(String traceId, String clientMsgId) {
        return UUID.nameUUIDFromBytes(("webhook-result:" + traceId + ":" + clientMsgId)
                .getBytes(StandardCharsets.UTF_8)).toString();
    }

    public Item loadPending(Map<String, AttributeValue> key, Instant now) {
        var stored = read(key);
        if (stored == null || stored.isEmpty() || !"PENDING".equals(stored.getOrDefault("status", s("")).s())
                || !stored.containsKey(MessageResultInboxIndex.DUE)
                || Long.parseLong(stored.get(MessageResultInboxIndex.DUE).n()) > now.toEpochMilli()) return null;
        var entry = new Item(stored.get("delivery_id").s(), stored.get("result_id").s(),
                stored.get("source").s(), stored.get("result_payload").s(),
                Instant.parse(stored.get("received_at").s()));
        if (!key.equals(entry.key())) throw new IllegalStateException("Result inbox key conflicts with payload");
        return entry;
    }

    public void defer(Item entry, Instant nextCheck) {
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(STEP).key(entry.key())
                    .conditionExpression("#status = :pending AND result_payload = :payload AND attribute_exists(#bucket)")
                    .updateExpression("SET #due = :due")
                    .expressionAttributeNames(Map.of("#status", "status", "#bucket", MessageResultInboxIndex.BUCKET,
                            "#due", MessageResultInboxIndex.DUE))
                    .expressionAttributeValues(Map.of(":pending", s("PENDING"), ":payload", s(entry.payload()),
                            ":due", AttributeValue.fromN(Long.toString(nextCheck.toEpochMilli())))).build());
        } catch (ConditionalCheckFailedException changed) {
            var current = read(entry.key());
            if (current == null || !"PROCESSED".equals(current.getOrDefault("status", s("")).s())) throw changed;
        }
    }

    private void put(Item entry) {
        var item = new HashMap<>(entry.key());
        item.put("schema_version", AttributeValue.fromN("4"));
        item.put("delivery_id", s(entry.clientMsgId()));
        item.put("result_id", s(entry.resultId()));
        item.put("source", s(entry.source()));
        item.put("result_payload", s(entry.payload()));
        item.put("status", s("PENDING"));
        item.put("received_at", s(entry.receivedAt().toString()));
        item.put("captured_at", s(clock.instant().toString()));
        // A very late provider webhook can arrive after completion has removed ORIGIN.
        item.put("ttl_epoch_seconds", AttributeValue.fromN(Long.toString(
                clock.instant().plus(Duration.ofDays(7)).getEpochSecond())));
        MessageResultInboxIndex.add(item, entry.clientMsgId(), entry.receivedAt().toEpochMilli());
        try {
            db.putItem(PutItemRequest.builder().tableName(STEP).item(item)
                    .conditionExpression("attribute_not_exists(pk)").build());
        } catch (ConditionalCheckFailedException replay) {
            var existing = read(entry.key());
            if (!entry.source().equals(existing.getOrDefault("source", s("")).s())
                    || !entry.payload().equals(existing.getOrDefault("result_payload", s("")).s())) {
                throw new IllegalStateException("Conflicting MSG_RESULT item identity", replay);
            }
        }
    }

    public void processed(Item entry) {
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(STEP).key(entry.key())
                    .conditionExpression("#status = :pending AND result_payload = :payload")
                    .updateExpression("SET #status = :processed, processed_at = :now REMOVE #bucket, #due")
                    .expressionAttributeNames(Map.of("#status", "status", "#bucket", MessageResultInboxIndex.BUCKET,
                            "#due", MessageResultInboxIndex.DUE))
                    .expressionAttributeValues(Map.of(":pending", s("PENDING"), ":payload", s(entry.payload()),
                            ":processed", s("PROCESSED"), ":now", s(clock.instant().toString()))).build());
        } catch (ConditionalCheckFailedException replay) {
            var existing = read(entry.key());
            if (!"PROCESSED".equals(existing.getOrDefault("status", s("")).s())
                    || !entry.payload().equals(existing.getOrDefault("result_payload", s("")).s())) throw replay;
        }
    }

    private Map<String, AttributeValue> read(Map<String, AttributeValue> key) {
        return db.getItem(GetItemRequest.builder().tableName(STEP).key(key).consistentRead(true).build()).item();
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

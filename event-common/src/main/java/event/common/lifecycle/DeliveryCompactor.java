package event.common.lifecycle;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import tools.jackson.databind.json.JsonMapper;
import java.time.Instant;
import java.util.*;
import static event.common.dynamodb.DynamoDbAttributeNames.*;
import static event.common.dynamodb.DynamoDbTableNames.DELIVERY_STATE;
import static event.common.lifecycle.DeliveryCompletion.*;

/** Caller must first obtain this exact result from committed SQL history + notification + cleanup reservation. */
public class DeliveryCompactor {
    private final DynamoDbClient db;
    private final JsonMapper mapper = JsonMapper.builder().build();
    public DeliveryCompactor(DynamoDbClient db) { this.db = db; }

    public boolean compact(DeliveryFinalized result) {
        var meta = read(result.deliveryId(), "META");
        String resultHash = hash("result:v1", mapper.writeValueAsString(result));
        if (compacted(meta)) {
            if (!result.eventId().equals(value(meta, FENCE)) || !resultHash.equals(value(meta, "result_hash")))
                throw new IllegalStateException("Compacted completion disagrees with SQL result");
            return false;
        }
        if (!result.eventId().equals(value(meta, FENCE)) || !meta.containsKey(PAYLOAD))
            throw new IllegalStateException("Completion fence or original request missing; retain legacy data");
        var finalItem = read(result.deliveryId(), "FINAL");
        if (!"PUBLISHED".equals(value(finalItem, "publish_state"))
                || !result.equals(mapper.readValue(value(finalItem, "result_event"), DeliveryFinalized.class)))
            throw new IllegalStateException("Published FINAL does not match committed SQL result");
        var page = db.query(r -> r.tableName(DELIVERY_STATE).consistentRead(true)
                .overrideConfiguration(c -> c.apiCallTimeout(java.time.Duration.ofSeconds(10)).apiCallAttemptTimeout(java.time.Duration.ofSeconds(5)))
                .projectionExpression("pk, sk, attempt_id, #version, lifecycle_closed, receipt_tracking_version, receipt_marker_ids, receipt_event_id")
                .expressionAttributeNames(Map.of("#version", VERSION))
                .keyConditionExpression("pk = :pk").expressionAttributeValues(Map.of(":pk", s("DELIVERY#" + result.deliveryId())))
                .limit(100));
        if (!page.lastEvaluatedKey().isEmpty()) throw new IllegalStateException("Unexpected delivery partition size; retain for review");
        var attempts = page.items().stream().filter(i -> value(i, SK).startsWith("ATTEMPT#")).toList();
        if (page.items().size() != attempts.size() + 2 || attempts.size() != result.routeOrder()
                || attempts.stream().noneMatch(i -> result.attemptId().equals(value(i, ATTEMPT_ID))))
            throw new IllegalStateException("Unexpected execution items; retain for review");
        var writes = new ArrayList<TransactWriteItem>();
        var compact = new HashMap<>(metaKey(result.deliveryId()));
        for (String field : List.of(DELIVERY_ID, EVENT_ID, TENANT_ID, DELIVERY_TYPE, OCCURRED_AT)) {
            if (!meta.containsKey(field)) throw new IllegalStateException("Incomplete original request metadata");
            compact.put(field, meta.get(field));
        }
        compact.put(STATUS, s("COMPLETED")); compact.put(FENCE, s(result.eventId()));
        compact.put("fingerprint_version", AttributeValue.fromN("1"));
        compact.put(FINGERPRINT, s(fingerprint(result.deliveryId(), Long.parseLong(meta.get(TENANT_ID).n()), value(meta, DELIVERY_TYPE),
                Boolean.TRUE.equals(meta.getOrDefault(FALLBACK_ALLOWED, AttributeValue.fromBool(false)).bool()), value(meta, PAYLOAD))));
        compact.put("result_hash", s(resultHash)); compact.put("final_outcome", s(result.outcome()));
        compact.put("finalized_at", s(result.finalizedAt().toString())); compact.put("compacted_at", s(Instant.now().toString()));
        writes.add(TransactWriteItem.builder().put(Put.builder().tableName(DELIVERY_STATE).item(compact)
                .conditionExpression("completion_event_id = :event AND payload = :payload")
                .expressionAttributeValues(Map.of(":event", s(result.eventId()), ":payload", meta.get(PAYLOAD))).build()).build());
        writes.add(TransactWriteItem.builder().delete(Delete.builder().tableName(DELIVERY_STATE).key(key(result.deliveryId(), "FINAL"))
                .conditionExpression("publish_state = :published AND result_event = :event")
                .expressionAttributeValues(Map.of(":published", s("PUBLISHED"), ":event", finalItem.get("result_event"))).build()).build());
        var receiptIds = new HashSet<String>();
        for (var attempt : attempts) {
            if (!Boolean.TRUE.equals(attempt.getOrDefault("lifecycle_closed", AttributeValue.fromBool(false)).bool())
                    || !"1".equals(attempt.getOrDefault(TRACKING_VERSION, AttributeValue.fromN("0")).n()))
                throw new IllegalStateException("Unclosed or legacy untracked attempt; retain all data");
            writes.add(TransactWriteItem.builder().delete(Delete.builder().tableName(DELIVERY_STATE).key(key(result.deliveryId(), value(attempt, SK)))
                    .conditionExpression("#version = :version AND lifecycle_closed = :closed")
                    .expressionAttributeNames(Map.of("#version", VERSION))
                    .expressionAttributeValues(Map.of(":version", attempt.get(VERSION), ":closed", AttributeValue.fromBool(true))).build()).build());
            receiptIds.addAll(attempt.getOrDefault(RECEIPT_IDS, AttributeValue.fromSs(List.of())).ss());
            if (attempt.containsKey("receipt_event_id") && !receiptIds.contains(value(attempt, "receipt_event_id")))
                throw new IllegalStateException("Incomplete receipt marker manifest; retain all data");
        }
        for (String receipt : receiptIds) {
            writes.add(TransactWriteItem.builder().delete(Delete.builder().tableName(DELIVERY_STATE)
                    .key(Map.of(PK, s("RECEIPT#" + receipt), SK, s("META")))
                    .conditionExpression("attribute_not_exists(pk) OR delivery_id = :delivery")
                    .expressionAttributeValues(Map.of(":delivery", s(result.deliveryId()))).build()).build());
        }
        if (writes.size() > 100) throw new IllegalStateException("Cleanup exceeds DynamoDB transaction size");
        db.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writes)
                .overrideConfiguration(c -> c.apiCallTimeout(java.time.Duration.ofSeconds(10)).apiCallAttemptTimeout(java.time.Duration.ofSeconds(5))).build());
        return true;
    }
    private Map<String, AttributeValue> read(String delivery, String sk) {
        return db.getItem(r -> r.tableName(DELIVERY_STATE).key(key(delivery, sk)).consistentRead(true)
                .overrideConfiguration(c -> c.apiCallTimeout(java.time.Duration.ofSeconds(10)).apiCallAttemptTimeout(java.time.Duration.ofSeconds(5)))).item();
    }
    private static Map<String, AttributeValue> key(String delivery, String sk) { return Map.of(PK, s("DELIVERY#" + delivery), SK, s(sk)); }
    private static String value(Map<String, AttributeValue> item, String attr) { return item.getOrDefault(attr, s("")).s(); }
    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

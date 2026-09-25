package event.delivery.dispatch.lifecycle;

import event.common.delivery.DeliveryEvent;
import event.common.lifecycle.DeliveryFinalized;
import event.common.lifecycle.LifecycleIndex;
import event.common.lifecycle.DeliveryCompletion;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.*;

import static event.common.dynamodb.DynamoDbAttributeNames.*;
import static event.common.dynamodb.DynamoDbTableNames.DELIVERY_STATE;

public class LifecycleRepository {
    private final DynamoDbClient db;
    private final JsonMapper mapper;
    public LifecycleRepository(DynamoDbClient db, JsonMapper mapper) { this.db = db; this.mapper = mapper; }

    public Map<String, AttributeValue> read(String delivery, String sk) {
        var item = db.getItem(r -> r.tableName(DELIVERY_STATE).key(key(delivery, sk)).consistentRead(true)).item();
        if (!sk.equals("FINAL") || !item.isEmpty()) return item;
        return db.query(r -> r.tableName(DELIVERY_STATE).consistentRead(true)
                .keyConditionExpression("pk = :pk AND begins_with(sk, :prefix)")
                .expressionAttributeValues(Map.of(":pk", s("DELIVERY#" + delivery), ":prefix", s("ATTEMPT#"))))
                .items().stream().filter(i -> i.containsKey("result_event")).findFirst().orElse(Map.of());
    }
    public void requireNoOtherPrimary(String delivery, String expectedId) {
        var items = db.query(r -> r.tableName(DELIVERY_STATE).consistentRead(true)
                .keyConditionExpression("pk = :pk AND begins_with(sk, :prefix)")
                .expressionAttributeValues(Map.of(":pk", s("DELIVERY#" + delivery), ":prefix", s("ATTEMPT#")))).items();
        if (items.stream().anyMatch(i -> route(i) == 1 && !expectedId.equals(text(i, ATTEMPT_ID))))
            throw new IllegalStateException("Persisted primary provider differs from configuration");
    }
    public QueryResponse due(int shard, Instant now, int limit, Map<String, AttributeValue> cursor) {
        return db.query(r -> r.tableName(DELIVERY_STATE).indexName(LifecycleIndex.NAME)
                .keyConditionExpression("lifecycle_bucket = :bucket AND lifecycle_due <= :now")
                .expressionAttributeValues(Map.of(":bucket", s(LifecycleIndex.bucket(shard)), ":now", n(now.toEpochMilli())))
                .limit(limit).exclusiveStartKey(cursor.isEmpty() ? null : cursor));
    }
    public void requireIndex() {
        var index = db.describeTable(r -> r.tableName(DELIVERY_STATE)).table().globalSecondaryIndexes().stream()
                .filter(i -> i.indexName().equals(LifecycleIndex.NAME)).findFirst().orElseThrow(() ->
                        new IllegalStateException("Install lifecycle_due_v1 before enabling lifecycle worker"));
        if (index.indexStatus() != IndexStatus.ACTIVE || index.projection().projectionType() != ProjectionType.KEYS_ONLY
                || !index.keySchema().equals(LifecycleIndex.definition().keySchema()))
            throw new IllegalStateException("Lifecycle index is not active or has an incompatible schema");
    }

    public boolean expire(String delivery, Map<String, AttributeValue> item, Instant now) {
        if (!Set.of("PROCESSING", "ACCEPTED", "REVIEW_REQUIRED", "RETRY_SCHEDULED").contains(text(item, STATUS))) return false;
        Instant deadline = deadline(item);
        if (now.isBefore(deadline)) return false;
        int route = route(item);
        try {
            db.updateItem(r -> r.tableName(DELIVERY_STATE).key(key(delivery, text(item, SK)))
                    .conditionExpression("#status = :old AND #version = :version AND #deadline <= :now")
                    .updateExpression("SET #status = :pending, #version = #version + :one, failure_reason = :reason, "
                            + "failure_observed_at = :at, updated_at = :at, lifecycle_due = :zero REMOVE lease_until, next_attempt_at")
                    .expressionAttributeNames(Map.of("#status", STATUS, "#version", VERSION,
                            "#deadline", item.containsKey(DEADLINE_AT) ? DEADLINE_AT : PRIMARY_DEADLINE))
                    .expressionAttributeValues(Map.of(":old", item.get(STATUS), ":version", item.get(VERSION),
                            ":now", n(now.toEpochMilli()), ":pending", s("DECISION_PENDING"), ":one", n(1),
                            ":reason", s(route == 1 ? "PRIMARY_EXPIRED" : "SECONDARY_EXPIRED"),
                            ":at", s(deadline.toString()), ":zero", n(0))));
            return true;
        } catch (ConditionalCheckFailedException race) { return false; }
    }

    /** Once an Attempt exists, its own durable index replaces the pre-dispatch recovery entry. */
    public void releaseMeta(String delivery, Map<String, AttributeValue> meta) {
        if (!meta.containsKey(LifecycleIndex.BUCKET) || "2".equals(meta.getOrDefault("schema_version", n(1)).n())) return;
        db.updateItem(r -> r.tableName(DELIVERY_STATE).key(key(delivery, "META"))
                .conditionExpression("attribute_exists(pk)")
                .updateExpression("REMOVE lifecycle_bucket, lifecycle_due"));
    }

    public void waitForSecondary(String delivery, Map<String, AttributeValue> parent, Map<String, AttributeValue> child) {
        long due = deadline(child).toEpochMilli();
        if (parent.containsKey(LifecycleIndex.DUE) && Long.parseLong(parent.get(LifecycleIndex.DUE).n()) == due) return;
        try {
            db.updateItem(r -> r.tableName(DELIVERY_STATE).key(key(delivery, text(parent, SK)))
                    .conditionExpression("#version = :version AND secondary_attempt_id = :child AND attribute_not_exists(lifecycle_closed)")
                    .updateExpression("SET lifecycle_due = :due")
                    .expressionAttributeNames(Map.of("#version", VERSION))
                    .expressionAttributeValues(Map.of(":version", parent.get(VERSION), ":child", child.get(ATTEMPT_ID), ":due", n(due))));
        } catch (ConditionalCheckFailedException race) { /* finalization/receipt owns the next decision */ }
    }

    public boolean finalizeDelivery(DeliveryEvent event, Map<String, AttributeValue> parent,
                                    Map<String, AttributeValue> terminal, Instant now) {
        String state = text(terminal, STATUS);
        if (!Set.of("DELIVERED", "DECISION_PENDING").contains(state)) throw new IllegalArgumentException("Nonterminal attempt");
        String reason = state.equals("DELIVERED") ? "DELIVERED" : text(terminal, FAILURE_REASON);
        String outcome = state.equals("DELIVERED") ? "DELIVERED" : reason.endsWith("_EXPIRED") ? "EXPIRED" : "FAILED";
        Instant resultAt = Instant.parse(text(terminal, state.equals("DELIVERED") ? "receipt_received_at" : FAILURE_OBSERVED_AT));
        var result = new DeliveryFinalized(event.schemaVersion(), "DeliveryFinalized", DeliveryFinalized.eventId(event.deliveryId()), event.deliveryId(),
                event.tenantId(), event.deliveryType(), outcome, reason, route(terminal), text(terminal, ATTEMPT_ID),
                text(terminal, PROVIDER), event.occurredAt(), resultAt, now, deadline(terminal), event.requestKey());
        var finalItem = new HashMap<>(key(event.deliveryId(), "FINAL"));
        finalItem.put("request_key", s(event.requestKey()));
        finalItem.put(DELIVERY_ID, s(event.deliveryId())); finalItem.put("result_event", s(mapper.writeValueAsString(result)));
        finalItem.put("publish_state", s("PENDING")); finalItem.put(CREATED_AT, s(now.toString()));
        LifecycleIndex.add(finalItem, event.deliveryId(), 0);
        var writes = new ArrayList<TransactWriteItem>();
        writes.add(TransactWriteItem.builder().update(Update.builder().tableName(DELIVERY_STATE).key(key(event.requestKey(), "META"))
                .conditionExpression("delivery_id = :execution AND attribute_not_exists(completion_event_id)")
                .updateExpression("SET completion_event_id = :event REMOVE lifecycle_bucket, lifecycle_due")
                .expressionAttributeValues(Map.of(":event", s(result.eventId()), ":execution", s(event.deliveryId()))).build()).build());
        if (event.schemaVersion() < 2) writes.add(TransactWriteItem.builder().put(Put.builder().tableName(DELIVERY_STATE).item(finalItem)
                .conditionExpression("attribute_not_exists(pk)").build()).build());
        writes.add(closeAttempt(event.deliveryId(), terminal, null, event.schemaVersion() >= 2 ? result : null));
        if (route(terminal) == 2) writes.add(closeAttempt(event.deliveryId(), parent, text(terminal, ATTEMPT_ID), null));
        try {
            db.transactWriteItems(r -> r.transactItems(writes));
            return true;
        } catch (TransactionCanceledException race) {
            if (race.cancellationReasons().stream().noneMatch(c -> Set.of("ConditionalCheckFailed", "TransactionConflict").contains(c.code()))) throw race;
            return false;
        }
    }

    private TransactWriteItem closeAttempt(String delivery, Map<String, AttributeValue> item, String child, DeliveryFinalized result) {
        var values = new HashMap<>(Map.of(":version", item.get(VERSION), ":status", item.get(STATUS), ":closed", AttributeValue.fromBool(true)));
        String condition = "#version = :version AND #status = :status AND attribute_not_exists(lifecycle_closed)";
        if (child != null) { condition += " AND secondary_attempt_id = :child"; values.put(":child", s(child)); }
        String update = "SET lifecycle_closed = :closed REMOVE lifecycle_bucket, lifecycle_due";
        if (result != null) {
            values.put(":result", s(mapper.writeValueAsString(result))); values.put(":pending", s("PENDING"));
            values.put(":bucket", s(LifecycleIndex.bucket(delivery))); values.put(":zero", n(0));
            update = "SET lifecycle_closed = :closed, result_event = :result, publish_state = :pending, lifecycle_bucket = :bucket, lifecycle_due = :zero";
        }
        return TransactWriteItem.builder().update(Update.builder().tableName(DELIVERY_STATE).key(key(delivery, text(item, SK)))
                .conditionExpression(condition).updateExpression(update)
                .expressionAttributeNames(Map.of("#version", VERSION, "#status", STATUS)).expressionAttributeValues(values).build()).build();
    }

    public DeliveryFinalized result(Map<String, AttributeValue> item) { return mapper.readValue(text(item, "result_event"), DeliveryFinalized.class); }
    public void published(String delivery, String event, Instant now) {
        var publication = mapper.readValue(event, DeliveryFinalized.class);
        String publicationKey = publication.schemaVersion() >= 2 ? "ATTEMPT#" + publication.attemptId() : "FINAL";
        try {
            db.updateItem(r -> r.tableName(DELIVERY_STATE).key(key(delivery, publicationKey))
                    .conditionExpression("publish_state = :pending AND result_event = :event")
                    .updateExpression("SET publish_state = :done, published_at = :now REMOVE lifecycle_bucket, lifecycle_due")
                    .expressionAttributeValues(Map.of(":pending", s("PENDING"), ":done", s("PUBLISHED"), ":event", s(event), ":now", s(now.toString()))));
        } catch (ConditionalCheckFailedException race) {
            var current = read(delivery, "FINAL");
            var result = mapper.readValue(event, DeliveryFinalized.class);
            var meta = read(result.requestKey(), "META");
            if (result.schemaVersion() >= 2 && current.isEmpty()
                    && (meta.isEmpty() || !delivery.equals(text(meta, DELIVERY_ID)))) return;
            if (DeliveryCompletion.compacted(meta)
                    && mapper.readValue(event, DeliveryFinalized.class).eventId().equals(text(meta, DeliveryCompletion.FENCE))) return;
            if (!"PUBLISHED".equals(text(current, "publish_state")) || !event.equals(text(current, "result_event"))) throw race;
        }
    }
    public static Map<String, AttributeValue> key(String delivery, String sk) { return Map.of(PK, s("DELIVERY#" + delivery), SK, s(sk)); }
    public static String text(Map<String, AttributeValue> item, String attr) { return item.getOrDefault(attr, s("")).s(); }
    public static Instant deadline(Map<String, AttributeValue> item) {
        return Instant.ofEpochMilli(Long.parseLong(item.get(item.containsKey(DEADLINE_AT) ? DEADLINE_AT : PRIMARY_DEADLINE).n()));
    }
    public static int route(Map<String, AttributeValue> item) { return Integer.parseInt(item.getOrDefault(ROUTE_ORDER, n(1)).n()); }
    private static AttributeValue s(String s) { return AttributeValue.fromS(s); }
    private static AttributeValue n(long n) { return AttributeValue.fromN(Long.toString(n)); }
}

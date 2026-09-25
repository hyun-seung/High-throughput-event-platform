package event.delivery.dispatch.receipt;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryIds;
import event.common.lifecycle.DeliveryCompletion;
import event.common.lifecycle.DeliveryFinalized;
import event.common.receipt.ReceiptEvent;
import event.common.receipt.ReceiptOutcome;
import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.external.client.ProviderFailureException;
import event.delivery.dispatch.model.DispatchAttempt;
import event.delivery.dispatch.model.DispatchFailureDecision;
import event.delivery.dispatch.service.DispatchRetryPolicy;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;

import static event.common.dynamodb.DynamoDbAttributeNames.*;
import static event.common.dynamodb.DynamoDbTableNames.*;

@Repository
public class ReceiptResultRepository {
    private final DynamoDbClient db;
    private final DispatchProperties properties;
    private final JsonMapper mapper;

    public ReceiptResultRepository(DynamoDbClient db, DispatchProperties properties, JsonMapper mapper) {
        this.db = db;
        this.properties = properties;
        this.mapper = mapper;
    }

    private event.common.redis.DeliveryCache cache = event.common.redis.DeliveryCache.UNAVAILABLE;
    @org.springframework.beans.factory.annotation.Autowired
    public void cache(event.common.redis.DeliveryCache cache) { this.cache = cache; }
    private event.delivery.dispatch.retry.RetryPublisher retries;
    @org.springframework.beans.factory.annotation.Autowired
    public void retryPublisher(event.delivery.dispatch.retry.RetryPublisher retries) { this.retries = retries; }

    public ReceiptApplication apply(ReceiptEvent receipt, Instant now) {
        validate(receipt);
        String fingerprint = fingerprint(receipt);
        var key = attemptKey(receipt.deliveryId(), receipt.attemptId());
        // Bound immediate CAS retries. The Kafka handler retains input if the race continues.
        for (int race = 0; race < 3; race++) {
            var item = read(key);
            if (item.isEmpty()) {
                return new ReceiptApplication("late_discarded", false);
            }
            int route = item.containsKey(ROUTE_ORDER) ? Integer.parseInt(item.get(ROUTE_ORDER).n()) : 1;
            if (!receipt.deliveryId().equals(item.get(DELIVERY_ID).s())
                    || !receipt.attemptId().equals(item.get(ATTEMPT_ID).s())
                    || !receipt.provider().equals(item.get(PROVIDER).s()) || receipt.routeOrder() != route) {
                throw new IllegalStateException("Receipt does not match stored provider and route");
            }
            String deadlineName = item.containsKey(DEADLINE_AT) ? DEADLINE_AT : PRIMARY_DEADLINE;
            Instant deadline = Instant.ofEpochMilli(Long.parseLong(item.get(deadlineName).n()));
            boolean expired = !now.isBefore(deadline) || !receipt.receivedAt().isBefore(deadline);
            boolean modern = "2".equals(item.getOrDefault("schema_version", number(1)).n());
            if (modern && expired) return new ReceiptApplication("late_discarded", false);
            var prior = modern ? (receipt.eventId().equals(item.getOrDefault("receipt_event_id", text("")).s())
                    ? Map.of("fingerprint", item.get("receipt_fingerprint"), "resume_dispatch", item.get("receipt_resume")) : Map.<String, AttributeValue>of())
                    : read(receiptKey(receipt.eventId()));
            if (!prior.isEmpty()) {
                if (fingerprint.equals(prior.get("fingerprint").s())) {
                    // A previously committed decision must finish its handoff even after a crash/deadline.
                    return new ReceiptApplication("duplicate", prior.get("resume_dispatch").bool());
                }
                if (expired) return new ReceiptApplication("late_discarded", false);
                throw new IllegalStateException("Provider receipt ID reused with different content; retain conflict");
            }
            if (expired) return new ReceiptApplication("late_discarded", false);

            int retryCount = Integer.parseInt(item.get(RETRY_COUNT).n());
            int invocation = retryCount + 1;
            if (receipt.invocation() == null && retryCount != 0) {
                throw new IllegalStateException("Legacy receipt cannot identify a retry invocation; retain for review");
            }
            int reported = receipt.invocation() == null ? 1 : receipt.invocation();
            if (reported < invocation) return new ReceiptApplication("stale_invocation", false);
            if (reported > invocation) throw new IllegalStateException("Receipt invocation has not been sent");
            String state = item.get(STATUS).s();
            if (Set.of("DELIVERED", "DECISION_PENDING").contains(state)
                    || (item.containsKey("receipt_invocation") && Integer.parseInt(item.get("receipt_invocation").n()) == invocation)) {
                return new ReceiptApplication("closed_invocation", false);
            }
            if (state.equals("RETRY_SCHEDULED") && receipt.outcome() == ReceiptOutcome.FAILED) {
                return new ReceiptApplication("retry_already_scheduled", false);
            }
            if (!Set.of("PROCESSING", "ACCEPTED", "REVIEW_REQUIRED", "RETRY_SCHEDULED").contains(state)) {
                throw new IllegalStateException("Unsupported receipt attempt state");
            }

            long version = Long.parseLong(item.get(VERSION).n());
            var attempt = new DispatchAttempt(receipt.deliveryId(), receipt.attemptId(), receipt.provider(),
                    version, retryCount, deadline, route);
            DispatchFailureDecision failure = receipt.outcome() == ReceiptOutcome.FAILED
                    ? DispatchRetryPolicy.decide(attempt, failureKind(receipt.code()), receipt.receivedAt(), properties) : null;
            if (modern && failure != null && failure.state() == DispatchFailureDecision.State.RETRY_SCHEDULED) {
                var active = loadDelivery(receipt.deliveryId());
                if (active.isEmpty()) return new ReceiptApplication("late_discarded", false);
                retries.publish(new event.delivery.dispatch.retry.RetryCommand(active.get(), attempt, retryCount + 1, failure.nextAttemptAt()));
                return new ReceiptApplication("retry_published", false);
            }
            String nextState = failure == null ? "DELIVERED" : failure.state().name();
            boolean resume = failure != null && (failure.state() == DispatchFailureDecision.State.RETRY_SCHEDULED
                    || (route == 1 && failure.reason().equals("FALLBACK_REQUIRED")));
            var names = new HashMap<>(Map.of("#state", STATUS, "#version", VERSION, "#updated", UPDATED_AT,
                    "#deadline", deadlineName, "#lease", LEASE_UNTIL, "#next", NEXT_ATTEMPT_AT));
            var values = new HashMap<>(Map.of(":state", text(state), ":version", number(version),
                    ":newState", text(nextState), ":one", number(1), ":now", text(now.toString()),
                    ":nowMs", number(now.toEpochMilli()), ":event", text(receipt.eventId()),
                    ":invocation", number(invocation), ":received", text(receipt.receivedAt().toString()),
                    ":occurred", text(receipt.occurredAt().toString())));
            values.put(":code", text(receipt.code()));
            values.put(":lifecycleDue", number(failure == null || failure.state() == DispatchFailureDecision.State.DECISION_PENDING ? 0
                    : failure.nextAttemptAt() != null ? failure.nextAttemptAt().toEpochMilli() : deadline.toEpochMilli()));
            String expression = "SET lifecycle_due = :lifecycleDue, #state = :newState, #version = #version + :one, #updated = :now, "
                    + "receipt_event_id = :event, receipt_invocation = :invocation, receipt_received_at = :received, "
                    + "provider_result_at = :occurred, receipt_code = :code";
            if (failure != null) {
                names.put("#reason", FAILURE_REASON);
                names.put("#observed", FAILURE_OBSERVED_AT);
                values.put(":reason", text(failure.reason()));
                expression += ", #reason = :reason, #observed = :received";
                if (failure.state() == DispatchFailureDecision.State.REVIEW_REQUIRED) {
                    names.put("#review", REVIEW_REASON);
                    expression += ", #review = :reason";
                }
            }
            if (failure != null && failure.nextAttemptAt() != null) {
                values.put(":due", number(failure.nextAttemptAt().toEpochMilli()));
                expression += ", #next = :due REMOVE #lease";
            } else {
                expression += " REMOVE #lease, #next";
            }
            var ledger = new HashMap<>(receiptKey(receipt.eventId()));
            if (!modern) {
                expression += " ADD receipt_marker_ids :markerIds";
                values.put(":markerIds", AttributeValue.fromSs(List.of(receipt.eventId())));
            } else {
                expression = expression.replace(" REMOVE", ", receipt_fingerprint = :fingerprint, receipt_resume = :resume REMOVE");
                values.put(":fingerprint", text(fingerprint)); values.put(":resume", AttributeValue.fromBool(resume));
            }
            ledger.put("fingerprint", text(fingerprint));
            ledger.put("resume_dispatch", AttributeValue.fromBool(resume));
            ledger.put(DELIVERY_ID, text(receipt.deliveryId()));
            ledger.put(ATTEMPT_ID, text(receipt.attemptId()));
            ledger.put("result_state", text(nextState));
            ledger.put(CREATED_AT, text(now.toString()));
            try {
                if (modern && failure == null) {
                    var active = loadDelivery(receipt.deliveryId());
                    if (active.isEmpty()) return new ReceiptApplication("late_discarded", false);
                    var event = active.get();
                    var result = new DeliveryFinalized(2, "DeliveryFinalized",
                            DeliveryFinalized.eventId(receipt.deliveryId()), receipt.deliveryId(), event.tenantId(),
                            event.deliveryType(), "DELIVERED", "DELIVERED", route, receipt.attemptId(), receipt.provider(),
                            event.occurredAt(), receipt.receivedAt(), now, deadline, event.requestKey());
                    values.put(":result", text(mapper.writeValueAsString(result))); values.put(":pending", text("PENDING"));
                    values.put(":closed", AttributeValue.fromBool(true));
                    expression = expression.replace(" REMOVE", ", result_event = :result, publish_state = :pending, lifecycle_closed = :closed REMOVE");
                    var writes = new ArrayList<TransactWriteItem>();
                    writes.add(TransactWriteItem.builder().update(Update.builder().tableName(ORIGIN)
                            .key(DeliveryCompletion.metaKey(event.requestKey()))
                            .conditionExpression("delivery_id = :execution AND attribute_not_exists(completion_event_id)")
                            .updateExpression("SET completion_event_id = :event REMOVE lifecycle_bucket, lifecycle_due")
                            .expressionAttributeValues(Map.of(":execution", text(event.deliveryId()), ":event", text(result.eventId()))).build()).build());
                    writes.add(TransactWriteItem.builder().update(Update.builder().tableName(STEP).key(key)
                            .conditionExpression("#state = :state AND #version = :version AND #deadline > :nowMs AND attribute_not_exists(lifecycle_closed)")
                            .updateExpression(expression).expressionAttributeNames(names).expressionAttributeValues(values).build()).build());
                    if (route == 2) {
                        String parentId = secondaryParent(receipt);
                        var parentKey = attemptKey(receipt.deliveryId(), parentId); var parent = read(parentKey);
                        writes.add(TransactWriteItem.builder().update(Update.builder().tableName(STEP).key(parentKey)
                                .conditionExpression("#version = :version AND secondary_attempt_id = :child AND attribute_not_exists(lifecycle_closed)")
                                .updateExpression("SET lifecycle_closed = :closed REMOVE lifecycle_bucket, lifecycle_due")
                                .expressionAttributeNames(Map.of("#version", VERSION))
                                .expressionAttributeValues(Map.of(":version", parent.get(VERSION), ":child", text(receipt.attemptId()), ":closed", AttributeValue.fromBool(true))).build()).build());
                    }
                    db.transactWriteItems(r -> r.transactItems(writes));
                    cache.schedule(receipt.deliveryId(), Instant.EPOCH);
                    return new ReceiptApplication("delivered", false);
                }
                if (modern) {
                    db.updateItem(UpdateItemRequest.builder().tableName(STEP).key(key)
                            .conditionExpression("#state = :state AND #version = :version AND #deadline > :nowMs")
                            .updateExpression(expression).expressionAttributeNames(names).expressionAttributeValues(values).build());
                    cache.schedule(receipt.deliveryId(), failure == null || failure.state() == DispatchFailureDecision.State.DECISION_PENDING
                            ? Instant.EPOCH : deadline);
                    return new ReceiptApplication(nextState.toLowerCase(Locale.ROOT), resume);
                }
                db.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(
                        TransactWriteItem.builder().put(Put.builder().tableName(STEP).item(ledger)
                                .conditionExpression("attribute_not_exists(pk)").build()).build(),
                        TransactWriteItem.builder().update(Update.builder().tableName(STEP).key(key)
                                .conditionExpression("#state = :state AND #version = :version AND #deadline > :nowMs")
                                .updateExpression(expression).expressionAttributeNames(names).expressionAttributeValues(values)
                                .build()).build()).build());
                return new ReceiptApplication(nextState.toLowerCase(Locale.ROOT), resume);
            } catch (ConditionalCheckFailedException changed) { /* retry the state read */
            } catch (TransactionCanceledException changed) {
                if (changed.cancellationReasons().stream().noneMatch(reason ->
                        "ConditionalCheckFailed".equals(reason.code()) || "TransactionConflict".equals(reason.code()))) throw changed;
            }
        }
        throw new IllegalStateException("Receipt state kept changing; retain input for retry");
    }

    public Optional<DeliveryEvent> loadDelivery(String deliveryId) {
        var item = read(DeliveryCompletion.metaKey(deliveryId));
        if (item.isEmpty()) {
            var page = db.query(r -> r.tableName(STEP).consistentRead(true)
                    .keyConditionExpression("pk = :pk")
                    .expressionAttributeValues(Map.of(":pk", text("DELIVERY#" + deliveryId))).limit(1));
            if (page.items().isEmpty() || !page.items().getFirst().containsKey("request_key")) return Optional.empty();
            item = read(DeliveryCompletion.metaKey(page.items().getFirst().get("request_key").s()));
            if (item.isEmpty() || !deliveryId.equals(item.get(DELIVERY_ID).s())) return Optional.empty();
        }
        if (DeliveryCompletion.compacted(item)) return Optional.empty();
        String execution = item.get(DELIVERY_ID).s();
        String requestKey = item.getOrDefault("request_key", text(execution)).s();
        int schema = Integer.parseInt(item.getOrDefault("schema_version", number(1)).n());
        var original = DeliveryEvent.requested(execution, Long.parseLong(item.get(TENANT_ID).n()), item.get(DELIVERY_TYPE).s(),
                mapper.readValue(item.get(PAYLOAD).s(), new TypeReference<Map<String, Object>>() {}),
                Instant.parse(item.get(OCCURRED_AT).s()),
                Boolean.TRUE.equals(item.getOrDefault(FALLBACK_ALLOWED, AttributeValue.fromBool(false)).bool()));
        return Optional.of(new DeliveryEvent(schema, original.eventId(), original.eventType(), execution, original.tenantId(),
                original.deliveryType(), original.payload(), original.occurredAt(), requestKey, null, original.fallbackAllowed(), requestKey)
                .toDispatchRequested());
    }

    public String secondaryParent(ReceiptEvent receipt) {
        String parentId = DeliveryIds.attemptId(receipt.deliveryId(), properties.provider(), 1, 1);
        var parent = read(attemptKey(receipt.deliveryId(), parentId));
        if (!receipt.attemptId().equals(parent.getOrDefault("secondary_attempt_id", text("")).s())
                || !receipt.provider().equals(parent.getOrDefault("secondary_provider", text("")).s())) {
            throw new IllegalStateException("Secondary receipt is not bound to the configured primary route");
        }
        return parentId;
    }

    private Map<String, AttributeValue> read(Map<String, AttributeValue> key) {
        return db.getItem(GetItemRequest.builder().tableName(tableForKey(key)).key(key).consistentRead(true).build()).item();
    }

    public static Map<String, AttributeValue> attemptKey(String deliveryId, String attemptId) {
        return Map.of(PK, text("DELIVERY#" + deliveryId), SK, text("ATTEMPT#" + attemptId));
    }

    public static Map<String, AttributeValue> receiptKey(String eventId) {
        return Map.of(PK, text("RECEIPT#" + eventId), SK, text("META"));
    }

    private static ProviderFailureException.Kind failureKind(String code) {
        return switch (code) {
            case "RETRY_1S" -> ProviderFailureException.Kind.RETRY_1S;
            case "RETRY_10S" -> ProviderFailureException.Kind.RETRY_10S;
            case "FALLBACK" -> ProviderFailureException.Kind.FALLBACK_REQUIRED;
            case "REJECTED" -> ProviderFailureException.Kind.PERMANENT_REJECTION;
            default -> ProviderFailureException.Kind.INVALID_RESPONSE;
        };
    }

    private static void validate(ReceiptEvent r) {
        if (r == null || r.eventId() == null || r.schemaVersion() != 1 || !"DeliveryReceiptReceived".equals(r.eventType())
                || r.receiptId() == null || !r.receiptId().matches("[A-Za-z0-9._:-]{1,128}")
                || r.provider() == null || !r.provider().matches("[A-Za-z0-9_-]{1,64}")
                || r.deliveryId() == null || r.attemptId() == null || r.outcome() == null
                || r.occurredAt() == null || r.receivedAt() == null || r.code() == null
                || !r.code().matches("[A-Z][A-Z0-9_]{0,63}") || r.routeOrder() < 1 || r.routeOrder() > 2
                || (r.invocation() != null && (r.invocation() < 1 || r.invocation() > 4))) {
            throw new IllegalArgumentException("Invalid receipt contract");
        }
        if ((r.outcome() == ReceiptOutcome.DELIVERED && !r.code().equals("DELIVERED"))
                || (r.outcome() == ReceiptOutcome.FAILED && Set.of("DELIVERED", "ACCEPTED", "RECEIVED").contains(r.code()))
                || !r.eventId().equals(ReceiptEvent.received(r.receiptId(), r.deliveryId(), r.attemptId(), r.provider(),
                r.routeOrder(), r.outcome(), r.code(), r.occurredAt(), r.receivedAt(), r.invocation()).eventId())
                || !r.attemptId().equals(DeliveryIds.attemptId(r.deliveryId(), r.provider(), r.routeOrder(), 1))) {
            throw new IllegalArgumentException("Inconsistent receipt identity or outcome");
        }
    }

    private String fingerprint(ReceiptEvent r) {
        // Exclude per-arrival receivedAt; retain reported time and all provider-supplied identity/result fields.
        byte[] canonical = mapper.writeValueAsBytes(List.of(r.receiptId(), r.deliveryId(), r.attemptId(), r.provider(),
                r.routeOrder(), r.outcome().name(), r.code(), r.occurredAt().toString(), r.invocation() == null ? "legacy" : r.invocation()));
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical)); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    private static AttributeValue text(String value) { return AttributeValue.fromS(value); }
    private static AttributeValue number(long value) { return AttributeValue.fromN(Long.toString(value)); }
}

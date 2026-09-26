package event.common.lifecycle;

import java.time.Instant;
import java.util.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/** An exceptional operator decision; never invokes a provider or extends a business deadline. */
public final class ManualResolution {
    public enum Decision { SUCCEEDED, FAILED }
    public enum Outcome { APPLIED, ALREADY_APPLIED, STALE_OR_INELIGIBLE, PENDING_RECONCILIATION }
    public record Target(long tenantId, UUID requestKey, UUID deliveryId, UUID attemptId, long expectedVersion, UUID actionId, Decision decision) {
        public Target {
            if (tenantId <= 0 || requestKey == null || deliveryId == null || attemptId == null || expectedVersion < 0 || actionId == null || decision == null)
                throw new IllegalArgumentException("Complete operator target required");
        }
    }
    private final DynamoDbClient db;
    public ManualResolution(DynamoDbClient db) { this.db = db; }
    public Outcome apply(Target target, Instant now) {
        var key = Map.of("pk", s("DELIVERY#" + target.deliveryId()), "sk", s("ATTEMPT#" + target.attemptId()));
        var step = db.getItem(r -> r.tableName("STEP").key(key).consistentRead(true)).item();
        if (target.actionId().toString().equals(text(step, "operator_action_id"))) return Outcome.ALREADY_APPLIED;
        if (!"2".equals(number(step, "schema_version")) || !target.deliveryId().toString().equals(text(step, "delivery_id"))
                || !target.attemptId().toString().equals(text(step, "attempt_id")) || !Long.toString(target.expectedVersion()).equals(number(step, "version"))
                || step.containsKey("lifecycle_closed") || step.containsKey("operator_action_id")) return Outcome.STALE_OR_INELIGIBLE;
        String route = number(step, "route_order");
        if (!Set.of("1", "2").contains(route)) return Outcome.STALE_OR_INELIGIBLE;
        var names = Map.of("#status", "status", "#version", "version");
        var values = new HashMap<String, AttributeValue>();
        values.put(":version", n(target.expectedVersion())); values.put(":one", n(1)); values.put(":two", n(2));
        values.put(":processing", s("PROCESSING")); values.put(":review", s("REVIEW_REQUIRED")); values.put(":now", n(now.toEpochMilli()));
        values.put(":state", s(target.decision() == Decision.SUCCEEDED ? "DELIVERED" : "DECISION_PENDING"));
        values.put(":at", s(now.toString())); values.put(":action", s(target.actionId().toString()));
        values.put(":decision", s(target.decision().name())); values.put(":bucket", s(LifecycleIndex.bucket(target.deliveryId().toString()))); values.put(":zero", n(0));
        String update = "SET #status=:state, #version=#version+:one, updated_at=:at, operator_action_id=:action, operator_decision=:decision, "
                + "operator_resolved_at=:at, lifecycle_bucket=:bucket, lifecycle_due=:zero";
        if (target.decision() == Decision.SUCCEEDED) update += ", receipt_received_at=:at REMOVE lease_until,next_attempt_at,review_reason,failure_reason,failure_observed_at";
        else {
            values.put(":reason", s(route.equals("1") ? "FALLBACK_REQUIRED" : "OPERATOR_CONFIRMED_FAILED"));
            update += ", failure_reason=:reason,failure_observed_at=:at REMOVE lease_until,next_attempt_at,review_reason";
        }
        var stepWrite = TransactWriteItem.builder().update(Update.builder().tableName("STEP").key(key)
                .conditionExpression("#version=:version AND schema_version=:two AND deadline_at>:now AND attribute_not_exists(lifecycle_closed) "
                        + "AND attribute_not_exists(operator_action_id) AND (#status=:review OR (#status=:processing AND lease_until<=:now))")
                .updateExpression(update).expressionAttributeNames(names).expressionAttributeValues(values).build()).build();
        var originCheck = TransactWriteItem.builder().conditionCheck(ConditionCheck.builder().tableName("ORIGIN")
                .key(DeliveryCompletion.metaKey(target.requestKey().toString()))
                .conditionExpression("delivery_id=:execution AND tenant_id=:tenant AND schema_version=:two AND attribute_not_exists(completion_event_id) AND attribute_not_exists(dlt_recovery_hold)")
                .expressionAttributeValues(Map.of(":execution", s(target.deliveryId().toString()), ":tenant", n(target.tenantId()), ":two", n(2))).build()).build();
        try {
            db.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(originCheck, stepWrite).build());
            return Outcome.APPLIED;
        } catch (TransactionCanceledException rejected) {
            if (rejected.cancellationReasons().stream().anyMatch(r -> "TransactionConflict".equals(r.code()))) throw rejected;
            if (rejected.cancellationReasons().stream().noneMatch(r -> "ConditionalCheckFailed".equals(r.code()))) throw rejected;
            var current = db.getItem(r -> r.tableName("STEP").key(key).consistentRead(true)).item();
            if (target.actionId().toString().equals(text(current, "operator_action_id"))) return Outcome.ALREADY_APPLIED;
            if (Long.toString(target.expectedVersion()).equals(number(current, "version"))
                    && !current.containsKey("lifecycle_closed") && !current.containsKey("operator_action_id")
                    && Set.of("PROCESSING", "REVIEW_REQUIRED").contains(text(current, "status"))) {
                var origin = db.getItem(r -> r.tableName("ORIGIN").key(DeliveryCompletion.metaKey(target.requestKey().toString())).consistentRead(true)).item();
                if (target.deliveryId().toString().equals(text(origin, "delivery_id")) && Long.toString(target.tenantId()).equals(number(origin, "tenant_id"))
                        && !origin.containsKey("completion_event_id")) {
                    // A previous timed-out call could still be in flight with an earlier :now value.
                    // Until version/state changes fence that call, do not certify a negative outcome.
                    return Outcome.PENDING_RECONCILIATION;
                }
            }
            return Outcome.STALE_OR_INELIGIBLE;
        }
    }
    private static String text(Map<String, AttributeValue> item, String key) { return item.getOrDefault(key, s("")).s(); }
    private static String number(Map<String, AttributeValue> item, String key) { return item.getOrDefault(key, n(-1)).n(); }
    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
    private static AttributeValue n(long value) { return AttributeValue.fromN(Long.toString(value)); }
}

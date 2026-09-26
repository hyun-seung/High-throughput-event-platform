package event.delivery.result.operations;

import event.common.lifecycle.DeliveryCompletion;
import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;

/** Metadata-only operational observations. No claims, retries, TTL extension, scans or provider calls. */
public final class DeliveryOperations {
    public record Page<T>(List<T> records, UUID nextAfterId, boolean hasMore) { }
    public record Notification(UUID batchId, long tenantId, int itemCount, int attempts, String lastError, Instant updatedAt) { }
    public record Cleanup(UUID resultEventId, UUID deliveryId, String requestKey, String attention, long version,
                          Instant nextAttemptAt, Instant leaseUntil, Instant createdAt) { }
    public record History(UUID deliveryId, UUID resultEventId, String outcome, String reason, Instant deadline,
                          String notificationState, Integer notificationAttempts, UUID batchId, String cleanupState) { }
    public record Attempt(String attemptId, String provider, String state, Long version, Long routeOrder, Long retryCount,
                          String reviewReason, String failureReason, Instant deadline, Instant leaseUntil,
                          Instant nextAttemptAt, String attention) { }
    public record Active(String observation, String executionId, boolean attemptsTruncated, List<Attempt> attempts) { }
    public record Request(long tenantId, String requestKey, Instant observedAt, Active active,
                          List<History> recentHistory, boolean historyTruncated) { }
    private final JdbcTemplate sql;
    private final DynamoDbClient db;
    private final Clock clock;
    public DeliveryOperations(JdbcTemplate sql, DynamoDbClient db, Clock clock) { this.sql = sql; this.db = db; this.clock = clock; }
    private static void bounds(long tenant, int limit) {
        if (tenant <= 0 || limit < 1 || limit > 100) throw new IllegalArgumentException("Tenant > 0 and limit 1..100 required");
    }
    private static Instant time(Timestamp value) { return value == null ? null : value.toInstant(); }
    public Page<Notification> exhausted(long tenant, UUID after, int limit) {
        bounds(tenant, limit);
        var rows = sql.query("SELECT batch_id,tenant_id,item_count,attempt_count,last_error,updated_at FROM customer_notification_batch "
                        + "WHERE tenant_id=? AND status='EXHAUSTED' " + (after == null ? "" : "AND batch_id>? ") + "ORDER BY batch_id LIMIT ?",
                (r, n) -> new Notification(r.getObject(1, UUID.class), r.getLong(2), r.getInt(3), r.getInt(4), r.getString(5), time(r.getTimestamp(6))),
                after == null ? new Object[]{tenant, limit + 1} : new Object[]{tenant, after, limit + 1});
        boolean more = rows.size() > limit; if (more) rows.removeLast();
        return new Page<>(List.copyOf(rows), more ? rows.getLast().batchId() : null, more);
    }
    public Page<Cleanup> cleanup(long tenant, UUID after, int limit) {
        bounds(tenant, limit); Instant now = clock.instant();
        var rows = sql.query("SELECT c.result_event_id,h.delivery_id,COALESCE(h.result_json->>'requestKey',h.delivery_id::text),"
                        + "c.next_attempt_at,c.lease_until,c.created_at,n.result_event_id AS notification_id,c.version,"
                        + "EXISTS(SELECT 1 FROM delivery_resolution_action a WHERE a.delivery_id=h.delivery_id AND a.tenant_id=h.tenant_id AND a.status='PENDING') AS action_pending FROM delivery_cleanup_outbox c "
                        + "JOIN delivery_history h ON h.result_event_id=c.result_event_id LEFT JOIN customer_notification_outbox n ON n.result_event_id=c.result_event_id "
                        + "WHERE h.tenant_id=? AND c.status='PENDING' " + (after == null ? "" : "AND c.result_event_id>? ") + "ORDER BY c.result_event_id LIMIT ?",
                (r, n) -> {
                    Instant due = time(r.getTimestamp(4)), lease = time(r.getTimestamp(5));
                    String attention = r.getObject(7) == null ? "MISSING_NOTIFICATION" : r.getBoolean(9) ? "RESOLUTION_PENDING" : lease != null && lease.isAfter(now) ? "LEASED"
                            : due.isAfter(now) ? "BACKOFF" : lease != null ? "LEASE_EXPIRED" : "DUE";
                    return new Cleanup(r.getObject(1, UUID.class), r.getObject(2, UUID.class), r.getString(3), attention, r.getLong(8), due, lease, time(r.getTimestamp(6)));
                }, after == null ? new Object[]{tenant, limit + 1} : new Object[]{tenant, after, limit + 1});
        boolean more = rows.size() > limit; if (more) rows.removeLast();
        return new Page<>(List.copyOf(rows), more ? rows.getLast().resultEventId() : null, more);
    }
    public Request request(long tenant, String requestKey) {
        bounds(tenant, 1); UUID.fromString(requestKey);
        if (db == null) throw new IllegalStateException("DynamoDB required for active execution lookup");
        Instant now = clock.instant();
        var history = sql.query("SELECT h.delivery_id,h.result_event_id,h.outcome,h.reason,h.deadline,n.status,n.attempt_count,n.batch_id,c.status "
                        + "FROM delivery_history h LEFT JOIN customer_notification_outbox n ON n.result_event_id=h.result_event_id "
                        + "LEFT JOIN delivery_cleanup_outbox c ON c.result_event_id=h.result_event_id "
                        + "WHERE h.tenant_id=? AND COALESCE(h.result_json->>'requestKey',h.delivery_id::text)=? ORDER BY h.stored_at DESC,h.delivery_id LIMIT 21",
                (r, n) -> new History(r.getObject(1, UUID.class), r.getObject(2, UUID.class), r.getString(3), r.getString(4), time(r.getTimestamp(5)),
                        r.getString(6), r.getObject(7, Integer.class), r.getObject(8, UUID.class), r.getString(9)), tenant, requestKey);
        boolean truncated = history.size() > 20; if (truncated) history.removeLast();
        return new Request(tenant, requestKey, now, active(tenant, requestKey, now), List.copyOf(history), truncated);
    }
    private Map<String, AttributeValue> origin(String key) {
        var names = projection("delivery_id", "tenant_id", "status", "completion_event_id", "dlt_recovery_hold");
        return db.getItem(r -> r.tableName("ORIGIN").key(DeliveryCompletion.metaKey(key)).consistentRead(true)
                .projectionExpression(String.join(",", names.keySet())).expressionAttributeNames(names)).item();
    }
    private Active active(long tenant, String requestKey, Instant now) {
        var before = origin(requestKey);
        if (before.isEmpty() || !Long.toString(tenant).equals(numberText(before, "tenant_id"))) return new Active("NO_ACTIVE_ORIGIN", null, false, List.of());
        String execution = text(before, "delivery_id"); UUID.fromString(execution);
        var names = projection("attempt_id", "provider", "status", "version", "route_order", "retry_count", "review_reason", "failure_reason",
                "deadline_at", "primary_deadline", "lease_until", "next_attempt_at");
        var steps = db.query(r -> r.tableName("STEP").consistentRead(true).keyConditionExpression("pk=:pk AND begins_with(sk,:prefix)")
                .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS("DELIVERY#" + execution), ":prefix", AttributeValue.fromS("ATTEMPT#")))
                .projectionExpression(String.join(",", names.keySet())).expressionAttributeNames(names).limit(100));
        if (!before.equals(origin(requestKey))) return new Active("CHANGED_DURING_READ", null, false, List.of());
        var attempts = steps.items().stream().map(item -> {
            String state = text(item, "status");
            Instant deadline = epoch(item, item.containsKey("deadline_at") ? "deadline_at" : "primary_deadline"), lease = epoch(item, "lease_until");
            String attention = deadline != null && !deadline.isAfter(now)
                    && Set.of("PROCESSING", "ACCEPTED", "REVIEW_REQUIRED", "RETRY_SCHEDULED").contains(state) ? "EXPIRY_DUE"
                    : "REVIEW_REQUIRED".equals(state) ? "REVIEW_REQUIRED"
                    : "PROCESSING".equals(state) && lease != null && !lease.isAfter(now) ? "RESULT_UNCONFIRMED" : "NONE";
            return new Attempt(text(item, "attempt_id"), text(item, "provider"), state, number(item, "version"), number(item, "route_order"),
                    number(item, "retry_count"), text(item, "review_reason"), text(item, "failure_reason"), deadline, lease, epoch(item, "next_attempt_at"), attention);
        }).toList();
        String observation = before.containsKey("dlt_recovery_hold") ? "RECOVERY_HELD" : before.containsKey("completion_event_id") ? "COMPLETION_RECORDED" : "ACTIVE";
        return new Active(observation, execution, steps.hasLastEvaluatedKey() && !steps.lastEvaluatedKey().isEmpty(), attempts);
    }
    private static Map<String, String> projection(String... attributes) {
        var names = new LinkedHashMap<String, String>();
        for (int i = 0; i < attributes.length; i++) names.put("#p" + i, attributes[i]);
        return names;
    }
    private static String text(Map<String, AttributeValue> item, String key) { return item.containsKey(key) ? item.get(key).s() : null; }
    private static String numberText(Map<String, AttributeValue> item, String key) { return item.containsKey(key) ? item.get(key).n() : null; }
    private static Long number(Map<String, AttributeValue> item, String key) { var n = numberText(item, key); return n == null ? null : Long.valueOf(n); }
    private static Instant epoch(Map<String, AttributeValue> item, String key) { var n = number(item, key); return n == null ? null : Instant.ofEpochMilli(n); }
}

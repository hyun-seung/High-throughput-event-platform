package messaging.complete;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;
import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

/** Cleans finalized DynamoDB rows after SQL commit and both Kafka handoffs are durable. */
@Component
@ConditionalOnProperty(prefix = "messaging.complete.cleanup", name = "enabled", havingValue = "true")
public class FinalizedDynamoCleanup {
    private static final Logger log = LoggerFactory.getLogger(FinalizedDynamoCleanup.class);
    private static final String TTL = "ttl_epoch_seconds";
    private final JdbcTemplate jdbc;
    private final DynamoDbClient db;
    private final Clock clock;
    private final int pageSize;
    private final int maxPagesPerPoll;
    private final Duration retention;

    @Autowired
    public FinalizedDynamoCleanup(JdbcTemplate jdbc, DynamoDbClient db,
                                  @Value("${messaging.complete.cleanup.page-size:100}") int pageSize,
                                  @Value("${messaging.complete.cleanup.max-pages-per-poll:4}") int maxPagesPerPoll,
                                  @Value("${messaging.complete.cleanup.retention:7d}") Duration retention) {
        this(jdbc, db, Clock.systemUTC(), pageSize, maxPagesPerPoll, retention);
    }

    FinalizedDynamoCleanup(JdbcTemplate jdbc, DynamoDbClient db, Clock clock,
                           int pageSize, int maxPagesPerPoll, Duration retention) {
        this.jdbc = Objects.requireNonNull(jdbc);
        this.db = Objects.requireNonNull(db);
        this.clock = Objects.requireNonNull(clock);
        if (pageSize < 1 || pageSize > 1000 || maxPagesPerPoll < 1 || maxPagesPerPoll > 20
                || retention == null || retention.isNegative()
                || retention.isZero()) throw new IllegalArgumentException("Invalid cleanup settings");
        this.pageSize = pageSize;
        this.maxPagesPerPoll = maxPagesPerPoll;
        this.retention = retention;
    }

    @Scheduled(initialDelayString = "${messaging.complete.cleanup.initial-delay-ms:5000}",
            fixedDelayString = "${messaging.complete.cleanup.poll-ms:5000}")
    public void poll() {
        // Drain a bounded number of full pages before sleeping, without adding concurrent writers.
        for (int page = 0; page < maxPagesPerPoll; page++) {
            if (cleanupPage() < pageSize) return;
        }
    }

    private int cleanupPage() {
        var pending = jdbc.query("""
                SELECT client_msg_id, decision_id, final_stage FROM tbl_msg_hist
                WHERE cleanup_status = 'PENDING' AND cleanup_next_at <= now()
                ORDER BY cleanup_next_at, client_msg_id LIMIT ?
                """, (rs, row) -> new Pending(rs.getString(1), rs.getString(2), rs.getString(3)), pageSize);
        for (var item : pending) {
            try {
                if (cleanup(item)) {
                    jdbc.update("""
                            UPDATE tbl_msg_hist SET cleanup_status = 'DONE', cleaned_at = now(),
                                cleanup_last_error = NULL WHERE client_msg_id = ? AND cleanup_status = 'PENDING'
                            """, item.clientMsgId());
                } else {
                    defer(item.clientMsgId(), null);
                }
            } catch (Exception failure) {
                log.warn("Finalized DynamoDB cleanup will retry: clientMsgId={}", item.clientMsgId(), failure);
                defer(item.clientMsgId(), failure.getClass().getSimpleName());
            }
        }
        return pending.size();
    }

    boolean cleanup(Pending item) {
        String id = item.clientMsgId();
        var originKey = key(id, "META");
        var origin = db.getItem(GetItemRequest.builder().tableName(ORIGIN).key(originKey)
                .consistentRead(true).build()).item();
        String outboxSk = ("SECONDARY".equals(item.stage())
                ? "SECONDARY_DECISION#" : "PRIMARY_DECISION#") + item.decisionId();
        var outbox = db.getItem(GetItemRequest.builder().tableName(STEP).key(key(id, outboxSk))
                .consistentRead(true).build()).item();
        if (!outbox.isEmpty() && !"PUBLISHED".equals(string(outbox, "status"))) return false;
        if (!origin.isEmpty()) {
            String decisionField = "SECONDARY".equals(item.stage())
                    ? "secondary_decision_id" : "primary_decision_id";
            if (!item.decisionId().equals(string(origin, decisionField))
                    || !isFinalStatus(string(origin, "status"), item.stage())) {
                throw new IllegalStateException("SQL final result differs from DynamoDB ORIGIN");
            }
            if (!origin.containsKey("cleanup_ready_at")) {
                if (!"PUBLISHED".equals(string(outbox, "status"))) return false;
                db.updateItem(UpdateItemRequest.builder().tableName(ORIGIN).key(originKey)
                        .conditionExpression("#status = :status AND #decision = :decision "
                                + "AND attribute_not_exists(cleanup_ready_at)")
                        .updateExpression("SET cleanup_ready_at = :now")
                        .expressionAttributeNames(Map.of("#status", "status", "#decision", decisionField))
                        .expressionAttributeValues(Map.of(":status", origin.get("status"),
                                ":decision", s(item.decisionId()), ":now", s(clock.instant().toString())))
                        .build());
            }
        }

        var steps = new ArrayList<Map<String, AttributeValue>>();
        Map<String, AttributeValue> cursor = null;
        do {
            var page = db.query(QueryRequest.builder().tableName(STEP).consistentRead(true)
                    .keyConditionExpression("pk = :pk")
                    .expressionAttributeValues(Map.of(":pk", s("DELIVERY#" + id)))
                    .exclusiveStartKey(cursor).limit(pageSize).build());
            for (var row : page.items()) steps.add(key(id, row.get("sk").s()));
            cursor = page.lastEvaluatedKey();
        } while (!cursor.isEmpty());

        long expiresAt = clock.instant().plus(retention).getEpochSecond();
        for (var step : steps) markTtl(STEP, step, expiresAt);
        if (!origin.isEmpty()) markTtl(ORIGIN, originKey, expiresAt);
        // Keep ORIGIN until every STEP delete is confirmed. Partial batches retain TTL protection
        // and return to the existing SQL retry schedule instead of blocking this worker in a retry loop.
        for (int start = 0; start < steps.size(); start += 25) {
            var deletes = steps.subList(start, Math.min(start + 25, steps.size())).stream()
                    .map(step -> WriteRequest.builder()
                            .deleteRequest(DeleteRequest.builder().key(step).build()).build())
                    .toList();
            var result = db.batchWriteItem(BatchWriteItemRequest.builder()
                    .requestItems(Map.of(STEP, deletes)).build());
            if (result.unprocessedItems().values().stream().anyMatch(items -> !items.isEmpty())) return false;
        }
        if (!origin.isEmpty()) db.deleteItem(DeleteItemRequest.builder().tableName(ORIGIN).key(originKey).build());
        return true;
    }

    private void markTtl(String table, Map<String, AttributeValue> key, long expiresAt) {
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(table).key(key)
                    .conditionExpression("attribute_exists(pk)")
                    .updateExpression("SET #ttl = :ttl")
                    .expressionAttributeNames(Map.of("#ttl", TTL))
                    .expressionAttributeValues(Map.of(":ttl", AttributeValue.fromN(Long.toString(expiresAt))))
                    .build());
        } catch (ConditionalCheckFailedException alreadyDeleted) {
            // Another cleanup worker may have removed this item.
        }
    }

    private void defer(String id, String error) {
        if (error == null) {
            jdbc.update("""
                    UPDATE tbl_msg_hist SET cleanup_next_at = now() + interval '10 seconds'
                    WHERE client_msg_id = ? AND cleanup_status = 'PENDING'
                    """, id);
            return;
        }
        jdbc.update("""
                UPDATE tbl_msg_hist SET cleanup_next_at = now()
                        + interval '10 seconds' * LEAST(30, cleanup_attempts + 1),
                    cleanup_attempts = cleanup_attempts + 1, cleanup_last_error = ?
                WHERE client_msg_id = ? AND cleanup_status = 'PENDING'
                """, error, id);
    }

    private static boolean isFinalStatus(String status, String stage) {
        return "SECONDARY".equals(stage)
                ? "SECONDARY_SUCCEEDED".equals(status) || "SECONDARY_FAILED".equals(status)
                : "PRIMARY_SUCCEEDED".equals(status) || "PRIMARY_FAILED".equals(status);
    }

    private static String string(Map<String, AttributeValue> item, String field) {
        return item == null || !item.containsKey(field) ? "" : item.get(field).s();
    }

    private static Map<String, AttributeValue> key(String id, String sk) {
        return Map.of("pk", s("DELIVERY#" + id), "sk", s(sk));
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }

    record Pending(String clientMsgId, String decisionId, String stage) { }
}

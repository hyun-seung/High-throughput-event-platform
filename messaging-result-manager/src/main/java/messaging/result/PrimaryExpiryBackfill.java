package messaging.result;

import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.PrimaryExpiryIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;

/** Explicit, bounded one-shot scan for active v4 messages admitted before the expiry index existed. */
@Component
@ConditionalOnProperty(prefix = "messaging.result.expiry.backfill", name = "enabled", havingValue = "true")
public class PrimaryExpiryBackfill {
    private static final Logger log = LoggerFactory.getLogger(PrimaryExpiryBackfill.class);
    private final DynamoDbClient db;
    private final int pageSize;
    private Map<String, AttributeValue> cursor = Map.of();
    private boolean complete;
    private long examined;
    private long indexed;

    public PrimaryExpiryBackfill(DynamoDbClient db,
                                 @Value("${messaging.result.expiry.backfill.page-size:100}") int pageSize) {
        this.db = Objects.requireNonNull(db);
        if (pageSize < 1 || pageSize > 1000) throw new IllegalArgumentException("Invalid expiry backfill page size");
        this.pageSize = pageSize;
    }

    @Scheduled(initialDelayString = "${messaging.result.expiry.backfill.initial-delay-ms:5000}",
            fixedDelayString = "${messaging.result.expiry.backfill.poll-ms:1000}")
    public synchronized void poll() {
        if (complete) return;
        var page = db.scan(ScanRequest.builder().tableName(ORIGIN).consistentRead(true)
                .projectionExpression("pk, sk, #status, schema_version, delivery_id, message_id, occurred_at, "
                        + "#bucket, #due, #deadline")
                .expressionAttributeNames(Map.of("#status", "status", "#bucket", PrimaryExpiryIndex.BUCKET,
                        "#due", PrimaryExpiryIndex.DUE, "#deadline", PrimaryExpiryIndex.DEADLINE))
                .exclusiveStartKey(cursor.isEmpty() ? null : cursor).limit(pageSize).build());
        for (var item : page.items()) {
            examined++;
            var update = plan(item);
            if (update == null) continue;
            try {
                db.updateItem(update);
                indexed++;
            } catch (ConditionalCheckFailedException changed) {
                var current = db.getItem(GetItemRequest.builder().tableName(ORIGIN)
                        .key(Map.of("pk", item.get("pk"), "sk", item.get("sk")))
                        .consistentRead(true).build()).item();
                if (plan(current) != null) throw changed;
            }
        }
        cursor = page.lastEvaluatedKey();
        if (cursor.isEmpty()) {
            complete = true;
            log.info("Primary expiry backfill complete: examined={}, indexed={}", examined, indexed);
        }
    }

    static UpdateItemRequest plan(Map<String, AttributeValue> item) {
        if (item == null || item.isEmpty() || !"META".equals(string(item, "sk"))
                || !string(item, "pk").startsWith("DELIVERY#")
                || !"4".equals(number(item, "schema_version"))
                || !MessageOriginCodec.STATUS_RECEIVED.equals(string(item, "status"))
                || !item.containsKey(MessageOriginCodec.MESSAGE_ID)) return null;
        if (item.containsKey(PrimaryExpiryIndex.BUCKET)
                || item.containsKey(PrimaryExpiryIndex.DUE)
                || item.containsKey(PrimaryExpiryIndex.DEADLINE)) {
            if (!item.containsKey(PrimaryExpiryIndex.BUCKET)
                    || !item.containsKey(PrimaryExpiryIndex.DUE)
                    || !item.containsKey(PrimaryExpiryIndex.DEADLINE)) {
                throw new IllegalStateException("Partial primary expiry index attributes on " + string(item, "pk"));
            }
            return null;
        }
        String id = string(item, "delivery_id");
        if (id.isBlank() || !("DELIVERY#" + id).equals(string(item, "pk"))) {
            throw new IllegalStateException("Invalid v4 ORIGIN key for expiry backfill");
        }
        var occurred = item.get("occurred_at");
        if (occurred == null) throw new IllegalStateException("Active v4 ORIGIN lacks occurred_at");
        long deadline = Instant.parse(occurred.s()).plus(PrimaryExpiryIndex.TTL).toEpochMilli();
        var values = Map.of(":received", s(MessageOriginCodec.STATUS_RECEIVED), ":v4", n(4),
                ":id", s(id), ":occurred", occurred,
                ":bucket", s(PrimaryExpiryIndex.bucket(id)), ":deadline", n(deadline));
        return UpdateItemRequest.builder().tableName(ORIGIN)
                .key(Map.of("pk", item.get("pk"), "sk", item.get("sk")))
                .conditionExpression("#status = :received AND schema_version = :v4 AND delivery_id = :id "
                        + "AND occurred_at = :occurred AND attribute_exists(message_id) "
                        + "AND attribute_not_exists(#bucket) AND attribute_not_exists(#due) "
                        + "AND attribute_not_exists(#deadline)")
                .updateExpression("SET #bucket = :bucket, #due = :deadline, #deadline = :deadline")
                .expressionAttributeNames(Map.of("#status", "status", "#bucket", PrimaryExpiryIndex.BUCKET,
                        "#due", PrimaryExpiryIndex.DUE, "#deadline", PrimaryExpiryIndex.DEADLINE))
                .expressionAttributeValues(values).build();
    }

    private static String string(Map<String, AttributeValue> item, String name) {
        return item.getOrDefault(name, s("")).s();
    }

    private static String number(Map<String, AttributeValue> item, String name) {
        return item.getOrDefault(name, n(0)).n();
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
    private static AttributeValue n(long value) { return AttributeValue.fromN(Long.toString(value)); }
}

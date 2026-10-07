package messaging.result;

import messaging.common.messages.PrimaryExpiryIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;

/** Rechecks 3-hour first-send expiry candidates against the current durable decision. */
@Component
public class PrimaryExpiryDispatcher {
    private static final Logger log = LoggerFactory.getLogger(PrimaryExpiryDispatcher.class);
    private final DynamoDbClient db;
    private final PrimaryStageDecisionStore decisions;
    private final Clock clock;
    private final int pageSize;
    private final Map<Integer, Map<String, AttributeValue>> cursors = new HashMap<>();

    public PrimaryExpiryDispatcher(DynamoDbClient db, PrimaryStageDecisionStore decisions, Clock clock,
                                   @Value("${messaging.result.expiry.page-size:100}") int pageSize) {
        this.db = Objects.requireNonNull(db);
        this.decisions = Objects.requireNonNull(decisions);
        this.clock = Objects.requireNonNull(clock);
        if (pageSize < 1 || pageSize > 1000) throw new IllegalArgumentException("Invalid expiry page size");
        this.pageSize = pageSize;
    }

    @Scheduled(initialDelayString = "${messaging.result.expiry.initial-delay-ms:5000}",
            fixedDelayString = "${messaging.result.expiry.poll-ms:5000}")
    public synchronized void poll() {
        Instant now = clock.instant();
        for (int shard = 0; shard < PrimaryExpiryIndex.SHARDS; shard++) pollShard(shard, now);
    }

    private void pollShard(int shard, Instant now) {
        var page = db.query(QueryRequest.builder().tableName(ORIGIN).indexName(PrimaryExpiryIndex.NAME)
                .keyConditionExpression("#bucket = :bucket AND #due <= :now")
                .expressionAttributeNames(Map.of("#bucket", PrimaryExpiryIndex.BUCKET,
                        "#due", PrimaryExpiryIndex.DUE))
                .expressionAttributeValues(Map.of(":bucket", s("message-primary-expiry-v1-" + shard),
                        ":now", AttributeValue.fromN(Long.toString(now.toEpochMilli()))))
                .exclusiveStartKey(cursors.getOrDefault(shard, Map.of())).limit(pageSize).build());
        for (var candidate : page.items()) {
            try {
                dispatch(candidate, now);
            } catch (Exception failure) {
                log.error("Primary expiry remains due: pk={}, sk={}",
                        candidate.get("pk"), candidate.get("sk"), failure);
            }
        }
        if (page.lastEvaluatedKey().isEmpty()) cursors.remove(shard);
        else cursors.put(shard, page.lastEvaluatedKey());
    }

    void dispatch(Map<String, AttributeValue> candidate, Instant now) {
        String pk = candidate.get("pk").s();
        if (!pk.startsWith("DELIVERY#") || !"META".equals(candidate.get("sk").s())) {
            throw new IllegalStateException("Primary expiry index points to a non-message ORIGIN");
        }
        String clientMsgId = pk.substring("DELIVERY#".length());
        var outcome = decisions.fromExpiry(clientMsgId);
        if (outcome == PrimaryStageDecisionStore.Outcome.WAITING) {
            decisions.deferExpiry(clientMsgId, now.plus(Duration.ofSeconds(10)));
        }
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

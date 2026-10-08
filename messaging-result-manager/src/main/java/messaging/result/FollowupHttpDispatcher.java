package messaging.result;

import messaging.common.messages.FollowupDispatchIndex;
import messaging.common.messages.FollowupHttpCommand;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;
import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

/** Publishes due, frozen commands; an unconfirmed Kafka send remains indexed for replay. */
public class FollowupHttpDispatcher {
    private static final Logger log = LoggerFactory.getLogger(FollowupHttpDispatcher.class);
    private final DynamoDbClient db;
    private final JsonMapper mapper;
    private final KafkaTemplate<String, HttpSendCommand> kafka;
    private final Clock clock;
    private final int pageSize;
    private final Map<Integer, Map<String, AttributeValue>> cursors = new HashMap<>();

    public FollowupHttpDispatcher(DynamoDbClient db, JsonMapper mapper,
                                  KafkaTemplate<String, HttpSendCommand> kafka, Clock clock, int pageSize) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
        this.kafka = Objects.requireNonNull(kafka);
        this.clock = Objects.requireNonNull(clock);
        if (pageSize < 1 || pageSize > 1000) throw new IllegalArgumentException("Invalid follow-up page size");
        this.pageSize = pageSize;
    }

    public synchronized void poll() throws ExecutionException, InterruptedException {
        long now = clock.millis();
        for (int shard = 0; shard < FollowupDispatchIndex.SHARDS; shard++) pollShard(shard, now);
    }

    private void pollShard(int shard, long now) throws ExecutionException, InterruptedException {
        var page = db.query(QueryRequest.builder().tableName(STEP).indexName(FollowupDispatchIndex.NAME)
                .keyConditionExpression("#bucket = :bucket AND #due <= :now")
                .expressionAttributeNames(Map.of("#bucket", FollowupDispatchIndex.BUCKET,
                        "#due", FollowupDispatchIndex.DUE))
                .expressionAttributeValues(Map.of(":bucket", s(FollowupDispatchIndex.bucket(shard)),
                        ":now", AttributeValue.fromN(Long.toString(now))))
                .exclusiveStartKey(cursors.get(shard)).limit(pageSize).build());
        for (var candidate : page.items()) {
            try {
                dispatch(candidate, now);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } catch (Exception failed) {
                log.error("Follow-up HTTP command remains due: pk={}, sk={}",
                        candidate.get("pk"), candidate.get("sk"), failed);
            }
        }
        if (page.lastEvaluatedKey().isEmpty()) cursors.remove(shard);
        else cursors.put(shard, page.lastEvaluatedKey());
    }

    void dispatch(Map<String, AttributeValue> candidate, long now) throws ExecutionException, InterruptedException {
        var key = Map.of("pk", candidate.get("pk"), "sk", candidate.get("sk"));
        var stored = read(STEP, key);
        if (stored == null || !stored.containsKey(FollowupDispatchIndex.BUCKET)) return;
        FollowupHttpCommand followup = mapper.readValue(stored.get("authorization").s(), FollowupHttpCommand.class);
        String clientMsgId = followup.command().request().clientMsgId();
        if (!key.equals(FollowupHttpCommand.key(followup.command()))
                || !followup.decisionId().equals(stored.get("decision_id").s())) {
            throw new IllegalStateException("Follow-up index points to a conflicting authorization");
        }
        if (now < followup.notBefore().toEpochMilli()) return;
        var origin = read(ORIGIN, MessageOriginCodec.key(clientMsgId));
        if (origin == null || !clientMsgId.equals(origin.getOrDefault("delivery_id", s("")).s())
                || !MessageOriginCodec.STATUS_RECEIVED.equals(origin.getOrDefault("status", s("")).s())
                || origin.containsKey("completion_event_id")
                || !followup.decisionId().equals(origin.getOrDefault(FollowupHttpCommand.CURRENT_DECISION, s("")).s())) {
            markDone(key, stored, now);
            return;
        }
        try {
            kafka.send(MessageTopics.httpSend(followup.command().carrier()), clientMsgId, followup.command()).get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
        markDone(key, stored, now);
    }

    private void markDone(Map<String, AttributeValue> key, Map<String, AttributeValue> stored, long now) {
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(STEP).key(key)
                    .conditionExpression("authorization = :authorization AND #bucket = :bucket AND #due = :due")
                    .updateExpression("SET published_at_ms = :published REMOVE #bucket, #due")
                    .expressionAttributeNames(Map.of("#bucket", FollowupDispatchIndex.BUCKET,
                            "#due", FollowupDispatchIndex.DUE))
                    .expressionAttributeValues(Map.of(
                            ":authorization", stored.get("authorization"),
                            ":bucket", stored.get(FollowupDispatchIndex.BUCKET),
                            ":due", stored.get(FollowupDispatchIndex.DUE),
                            ":published", AttributeValue.fromN(Long.toString(now)))).build());
        } catch (ConditionalCheckFailedException alreadyDone) {
            var current = read(STEP, key);
            if (current == null || !stored.get("authorization").equals(current.get("authorization"))
                    || current.containsKey(FollowupDispatchIndex.BUCKET)) throw alreadyDone;
        }
    }

    private Map<String, AttributeValue> read(String table, Map<String, AttributeValue> key) {
        return db.getItem(GetItemRequest.builder().tableName(table).key(key).consistentRead(true).build()).item();
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

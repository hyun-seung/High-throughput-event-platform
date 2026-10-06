package messaging.publisher;

import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessagePublicationIndex;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageTopics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;
import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

/** Replays origins left behind by an unconfirmed API Kafka send, using the original execution ID. */
@Slf4j
@Component
public class OriginPublicationRecovery {
    private final DynamoDbClient db;
    private final JsonMapper mapper;
    private final KafkaTemplate<String, MessageSubmission> kafka;
    private final Clock clock;
    private final int pageSize;
    private final Map<Integer, Map<String, AttributeValue>> cursors = new HashMap<>();

    @Autowired
    public OriginPublicationRecovery(DynamoDbClient db, KafkaTemplate<String, MessageSubmission> kafka,
                                     JsonMapper mapper,
                                     @Value("${messaging.publisher.recovery.page-size:100}") int pageSize) {
        this(db, kafka, mapper, Clock.systemUTC(), pageSize);
    }

    OriginPublicationRecovery(DynamoDbClient db, KafkaTemplate<String, MessageSubmission> kafka, JsonMapper mapper,
                              Clock clock, int pageSize) {
        if (pageSize < 1 || pageSize > 1000) throw new IllegalArgumentException("Recovery page size must be 1..1000");
        this.db = db;
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
        this.pageSize = pageSize;
    }

    @Scheduled(initialDelayString = "${messaging.publisher.recovery.initial-delay-ms:60000}",
            fixedDelayString = "${messaging.publisher.recovery.poll-ms:60000}")
    public synchronized void poll() {
        long now = clock.millis();
        for (int shard = 0; shard < MessagePublicationIndex.SHARDS; shard++) pollShard(shard, now);
    }

    private void pollShard(int shard, long now) {
        String bucket = MessagePublicationIndex.bucketForShard(shard);
        var request = QueryRequest.builder().tableName(ORIGIN).indexName(MessagePublicationIndex.NAME)
                .keyConditionExpression("#bucket = :bucket AND #due <= :now")
                .expressionAttributeNames(Map.of("#bucket", MessagePublicationIndex.BUCKET,
                        "#due", MessagePublicationIndex.DUE))
                .expressionAttributeValues(Map.of(":bucket", AttributeValue.fromS(bucket),
                        ":now", AttributeValue.fromN(Long.toString(now))))
                .exclusiveStartKey(cursors.getOrDefault(shard, Map.of()))
                .limit(pageSize).build();
        var page = db.query(request);
        for (var candidate : page.items()) recover(candidate);
        if (page.lastEvaluatedKey().isEmpty()) cursors.remove(shard);
        else cursors.put(shard, page.lastEvaluatedKey());
    }

    void recover(Map<String, AttributeValue> candidate) {
        String clientMsgId = candidate.get("delivery_id") == null
                ? candidate.get("pk").s().substring("DELIVERY#".length())
                : candidate.get("delivery_id").s();
        var current = db.getItem(GetItemRequest.builder().tableName(ORIGIN)
                .key(MessageOriginCodec.key(clientMsgId)).consistentRead(true).build()).item();
        if (current.isEmpty()) return;
        if (!current.containsKey(MessageOriginCodec.MESSAGE_ID)) return;
        if (current.containsKey("completion_event_id")
                || !MessageOriginCodec.STATUS_RECEIVED.equals(current.getOrDefault("status", AttributeValue.fromS("")).s())
                || hasStep(clientMsgId)) {
            clearRecoveryIndex(clientMsgId);
            return;
        }
        var event = MessageOriginCodec.decode(current, mapper);
        try {
            kafka.send(MessageTopics.RECEIVED, event.clientMsgId(), event).get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("ORIGIN recovery publication interrupted", interrupted);
        } catch (Exception unconfirmed) {
            throw new IllegalStateException("ORIGIN recovery publication not confirmed", unconfirmed);
        }
        log.info("ORIGIN publication recovery sent: clientMsgId={}", clientMsgId);
    }

    private boolean hasStep(String clientMsgId) {
        return !db.query(QueryRequest.builder().tableName(STEP).consistentRead(true)
                .keyConditionExpression("pk = :pk")
                .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS("DELIVERY#" + clientMsgId)))
                .limit(1).build()).items().isEmpty();
    }

    private void clearRecoveryIndex(String clientMsgId) {
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(ORIGIN).key(MessageOriginCodec.key(clientMsgId))
                    .conditionExpression("delivery_id = :execution AND attribute_exists(#bucket)")
                    .updateExpression("REMOVE #bucket, #due")
                    .expressionAttributeNames(Map.of("#bucket", MessagePublicationIndex.BUCKET,
                            "#due", MessagePublicationIndex.DUE))
                    .expressionAttributeValues(Map.of(":execution", AttributeValue.fromS(clientMsgId))).build());
        } catch (ConditionalCheckFailedException alreadyGone) {
            // A sender or cleanup may have removed the index first.
        }
    }
}

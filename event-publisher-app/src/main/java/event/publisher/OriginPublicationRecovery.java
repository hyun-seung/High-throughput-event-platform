package event.publisher;

import event.common.events.EventOriginCodec;
import event.common.events.EventPublicationIndex;
import event.common.events.EventSubmission;
import lombok.extern.slf4j.Slf4j;
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

import static event.common.dynamodb.DynamoDbTableNames.ORIGIN;
import static event.common.dynamodb.DynamoDbTableNames.STEP;

/** Replays origins whose Stream record was lost or aged out, using the original execution ID. */
@Slf4j
@Component
public class OriginPublicationRecovery {
    private final DynamoDbClient db;
    private final JsonMapper mapper;
    private final OriginStreamProcessor publisher;
    private final Clock clock;
    private final int pageSize;
    private final Map<Integer, Map<String, AttributeValue>> cursors = new HashMap<>();

    public OriginPublicationRecovery(DynamoDbClient db, KafkaTemplate<String, EventSubmission> kafka,
                                     JsonMapper mapper,
                                     @Value("${event.publisher.recovery.page-size:100}") int pageSize) {
        this(db, new OriginStreamProcessor(db, kafka, mapper), mapper, Clock.systemUTC(), pageSize);
    }

    OriginPublicationRecovery(DynamoDbClient db, OriginStreamProcessor publisher, JsonMapper mapper,
                              Clock clock, int pageSize) {
        if (pageSize < 1 || pageSize > 1000) throw new IllegalArgumentException("Recovery page size must be 1..1000");
        this.db = db;
        this.publisher = publisher;
        this.mapper = mapper;
        this.clock = clock;
        this.pageSize = pageSize;
    }

    @Scheduled(initialDelayString = "${event.publisher.recovery.initial-delay-ms:60000}",
            fixedDelayString = "${event.publisher.recovery.poll-ms:60000}")
    public synchronized void poll() {
        long now = clock.millis();
        for (int shard = 0; shard < EventPublicationIndex.SHARDS; shard++) pollShard(shard, now);
    }

    private void pollShard(int shard, long now) {
        String bucket = "event-publication-v1-" + shard;
        var request = QueryRequest.builder().tableName(ORIGIN).indexName(EventPublicationIndex.NAME)
                .keyConditionExpression("#bucket = :bucket AND #due <= :now")
                .expressionAttributeNames(Map.of("#bucket", EventPublicationIndex.BUCKET,
                        "#due", EventPublicationIndex.DUE))
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
        String executionId = candidate.get("delivery_id") == null
                ? candidate.get("pk").s().substring("DELIVERY#".length())
                : candidate.get("delivery_id").s();
        var current = db.getItem(GetItemRequest.builder().tableName(ORIGIN)
                .key(EventOriginCodec.key(executionId)).consistentRead(true).build()).item();
        if (current.isEmpty()) return;
        if (!current.containsKey(EventOriginCodec.CLIENT_EVENT_ID)) return;
        if (current.containsKey("completion_event_id")
                || !EventOriginCodec.STATUS_RECEIVED.equals(current.getOrDefault("status", AttributeValue.fromS("")).s())
                || hasStep(executionId)) {
            clearRecoveryIndex(executionId);
            return;
        }
        publisher.publishIfActive(EventOriginCodec.decode(current, mapper));
        log.info("ORIGIN publication recovery sent: executionId={}", executionId);
    }

    private boolean hasStep(String executionId) {
        return !db.query(QueryRequest.builder().tableName(STEP).consistentRead(true)
                .keyConditionExpression("pk = :pk")
                .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS("DELIVERY#" + executionId)))
                .limit(1).build()).items().isEmpty();
    }

    private void clearRecoveryIndex(String executionId) {
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(ORIGIN).key(EventOriginCodec.key(executionId))
                    .conditionExpression("delivery_id = :execution AND attribute_exists(#bucket)")
                    .updateExpression("REMOVE #bucket, #due")
                    .expressionAttributeNames(Map.of("#bucket", EventPublicationIndex.BUCKET,
                            "#due", EventPublicationIndex.DUE))
                    .expressionAttributeValues(Map.of(":execution", AttributeValue.fromS(executionId))).build());
        } catch (ConditionalCheckFailedException alreadyGone) {
            // A sender or cleanup may have removed the index first.
        }
    }
}

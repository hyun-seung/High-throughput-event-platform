package messaging.result;

import messaging.common.messages.FinalizedMessageResult;
import messaging.common.messages.MessageResultInboxIndex;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageTopics;
import messaging.common.messages.PrimaryStageDecision;
import messaging.common.messages.SecondarySendCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
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

import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

/** Publishes the frozen primary decision without depending on a live ORIGIN item. */
@Component
@ConditionalOnProperty(prefix = "messaging.result.outbox", name = "enabled", havingValue = "true")
public class PrimaryDecisionOutboxDispatcher {
    private static final Logger log = LoggerFactory.getLogger(PrimaryDecisionOutboxDispatcher.class);
    private final DynamoDbClient db;
    private final JsonMapper mapper;
    private final KafkaTemplate<String, Object> kafka;
    private final Clock clock;
    private final int pageSize;
    private final Map<Integer, Map<String, AttributeValue>> cursors = new HashMap<>();

    public PrimaryDecisionOutboxDispatcher(DynamoDbClient db, JsonMapper mapper,
                                           KafkaTemplate<String, Object> kafka, Clock clock,
                                           @Value("${messaging.result.outbox.page-size:100}") int pageSize) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
        this.kafka = Objects.requireNonNull(kafka);
        this.clock = Objects.requireNonNull(clock);
        if (pageSize < 1 || pageSize > 1000) throw new IllegalArgumentException("Invalid primary outbox page size");
        this.pageSize = pageSize;
    }

    @Scheduled(initialDelayString = "${messaging.result.outbox.initial-delay-ms:5000}",
            fixedDelayString = "${messaging.result.outbox.poll-ms:5000}")
    public synchronized void poll() throws InterruptedException {
        long now = clock.millis();
        for (int shard = 0; shard < MessageResultInboxIndex.SHARDS; shard++) pollShard(shard, now);
    }

    private void pollShard(int shard, long now) throws InterruptedException {
        var page = db.query(QueryRequest.builder().tableName(STEP).indexName(MessageResultInboxIndex.NAME)
                .keyConditionExpression("#bucket = :bucket AND #due <= :now")
                .expressionAttributeNames(Map.of("#bucket", MessageResultInboxIndex.BUCKET,
                        "#due", MessageResultInboxIndex.DUE))
                .expressionAttributeValues(Map.of(":bucket", s("message-result-v1-" + shard),
                        ":now", AttributeValue.fromN(Long.toString(now))))
                .exclusiveStartKey(cursors.getOrDefault(shard, Map.of())).limit(pageSize).build());
        for (var candidate : page.items()) {
            if (!candidate.get("sk").s().startsWith("PRIMARY_DECISION#")) continue;
            try {
                dispatch(Map.of("pk", candidate.get("pk"), "sk", candidate.get("sk")), now);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } catch (Exception failure) {
                log.error("Primary decision outbox remains due: pk={}, sk={}",
                        candidate.get("pk"), candidate.get("sk"), failure);
            }
        }
        if (page.lastEvaluatedKey().isEmpty()) cursors.remove(shard);
        else cursors.put(shard, page.lastEvaluatedKey());
    }

    void dispatch(Map<String, AttributeValue> key, long now) throws ExecutionException, InterruptedException {
        var item = read(key);
        if (item == null || !"PENDING".equals(item.getOrDefault("status", s("")).s())
                || !item.containsKey(MessageResultInboxIndex.BUCKET)
                || Long.parseLong(item.get(MessageResultInboxIndex.DUE).n()) > now) return;
        var decision = mapper.readValue(item.get("result_payload").s(), PrimaryStageDecision.class);
        var submission = mapper.readValue(item.get("submission_payload").s(), MessageSubmission.class);
        if (!key.get("pk").s().equals("DELIVERY#" + decision.clientMsgId())
                || !key.get("sk").s().equals("PRIMARY_DECISION#" + decision.decisionId())
                || !decision.clientMsgId().equals(submission.clientMsgId())) {
            throw new IllegalStateException("Primary decision outbox identity mismatch");
        }
        String topic = decision.secondaryRequired() ? MessageTopics.TCP_SEND : MessageTopics.MSG_RESULT_FINALIZED;
        Object value = decision.secondaryRequired()
                ? SecondarySendCommand.from(decision, submission)
                : new FinalizedMessageResult(decision, submission);
        try {
            kafka.send(topic, decision.clientMsgId(), value).get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
        markPublished(key, item, now);
    }

    private void markPublished(Map<String, AttributeValue> key, Map<String, AttributeValue> item, long now) {
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(STEP).key(key)
                    .conditionExpression("#status = :pending AND result_payload = :payload "
                            + "AND submission_payload = :submission AND #bucket = :bucket AND #due = :due")
                    .updateExpression("SET #status = :published, published_at_ms = :now REMOVE #bucket, #due")
                    .expressionAttributeNames(Map.of("#status", "status", "#bucket", MessageResultInboxIndex.BUCKET,
                            "#due", MessageResultInboxIndex.DUE))
                    .expressionAttributeValues(Map.of(":pending", s("PENDING"), ":published", s("PUBLISHED"),
                            ":payload", item.get("result_payload"), ":submission", item.get("submission_payload"),
                            ":bucket", item.get(MessageResultInboxIndex.BUCKET),
                            ":due", item.get(MessageResultInboxIndex.DUE),
                            ":now", AttributeValue.fromN(Long.toString(now)))).build());
        } catch (ConditionalCheckFailedException changed) {
            var latest = read(key);
            if (latest == null || !"PUBLISHED".equals(latest.getOrDefault("status", s("")).s())
                    || !item.get("result_payload").equals(latest.get("result_payload"))
                    || !item.get("submission_payload").equals(latest.get("submission_payload"))) throw changed;
        }
    }

    private Map<String, AttributeValue> read(Map<String, AttributeValue> key) {
        return db.getItem(GetItemRequest.builder().tableName(STEP).key(key).consistentRead(true).build()).item();
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

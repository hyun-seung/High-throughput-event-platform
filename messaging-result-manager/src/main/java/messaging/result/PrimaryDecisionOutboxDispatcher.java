package messaging.result;

import io.micrometer.core.instrument.MeterRegistry;
import messaging.common.metrics.ScheduledWorkMetrics;
import static messaging.common.metrics.ScheduledWorkMetrics.Phase.*;

import messaging.common.messages.CustomerWebhookSendCommand;
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
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

/** Publishes the frozen primary decision and independently resumes final and customer handoffs. */
@Component
@ConditionalOnProperty(prefix = "messaging.result.outbox", name = "enabled", havingValue = "true")
public class PrimaryDecisionOutboxDispatcher {
    private static final Logger log = LoggerFactory.getLogger(PrimaryDecisionOutboxDispatcher.class);
    private final DynamoDbClient db;
    private final JsonMapper mapper;
    private final KafkaTemplate<String, Object> kafka;
    private final Clock clock;
    private final int pageSize;
    private final ScheduledWorkMetrics metrics;
    private final Map<Integer, Map<String, AttributeValue>> cursors = new HashMap<>();

    public PrimaryDecisionOutboxDispatcher(DynamoDbClient db, JsonMapper mapper,
                                           KafkaTemplate<String, Object> kafka, Clock clock,
                                           @Value("${messaging.result.outbox.page-size:100}") int pageSize, MeterRegistry registry) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
        this.kafka = Objects.requireNonNull(kafka);
        this.clock = Objects.requireNonNull(clock);
        if (pageSize < 1 || pageSize > 1000) throw new IllegalArgumentException("Invalid primary outbox page size");
        this.pageSize = pageSize;
        this.metrics = new ScheduledWorkMetrics(registry, ScheduledWorkMetrics.Worker.RESULT_OUTBOX);
    }

    @Scheduled(initialDelayString = "${messaging.result.outbox.initial-delay-ms:5000}",
            fixedDelayString = "${messaging.result.outbox.poll-ms:5000}")
    public synchronized void poll() throws InterruptedException {
        try (var timing = metrics.start(POLL)) {
            long now = clock.millis();
            for (int shard = 0; shard < MessageResultInboxIndex.SHARDS; shard++) pollShard(shard, now);
        }
    }

    private void pollShard(int shard, long now) throws InterruptedException {
        QueryResponse page;
        try (var timing = metrics.start(QUERY)) {
            page = db.query(QueryRequest.builder().tableName(STEP).indexName(MessageResultInboxIndex.NAME)
                .keyConditionExpression("#bucket = :bucket AND #due <= :now")
                .expressionAttributeNames(Map.of("#bucket", MessageResultInboxIndex.BUCKET,
                        "#due", MessageResultInboxIndex.DUE))
                .expressionAttributeValues(Map.of(":bucket", s("message-result-v1-" + shard),
                        ":now", AttributeValue.fromN(Long.toString(now))))
                .exclusiveStartKey(cursors.get(shard)).limit(pageSize).build());
        }
        metrics.page(page.items().size(),
                (int) page.items().stream().filter(PrimaryDecisionOutboxDispatcher::eligible).count());
        for (var candidate : page.items()) {
            if (!eligible(candidate)) continue;
            try (var timing = metrics.start(ITEM)) {
                if (candidate.containsKey(MessageResultInboxIndex.DUE)) {
                    metrics.due(Long.parseLong(candidate.get(MessageResultInboxIndex.DUE).n()), clock.millis());
                }
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
        String status = item == null ? "" : item.getOrDefault("status", s("")).s();
        if (!("PENDING".equals(status) || "FINALIZED_PUBLISHED".equals(status))
                || !item.containsKey(MessageResultInboxIndex.BUCKET)
                || Long.parseLong(item.get(MessageResultInboxIndex.DUE).n()) > now) return;
        boolean secondary = key.get("sk").s().startsWith("SECONDARY_DECISION#");
        var finalized = secondary ? mapper.readValue(item.get("result_payload").s(), FinalizedMessageResult.class) : null;
        var decision = secondary ? finalized.decision()
                : mapper.readValue(item.get("result_payload").s(), PrimaryStageDecision.class);
        var submission = mapper.readValue(item.get("submission_payload").s(), MessageSubmission.class);
        if (!key.get("pk").s().equals("DELIVERY#" + decision.clientMsgId())
                || !key.get("sk").s().equals(secondary
                        ? "SECONDARY_DECISION#" + finalized.secondaryDecision().decisionId()
                        : "PRIMARY_DECISION#" + decision.decisionId())
                || !decision.clientMsgId().equals(submission.clientMsgId())) {
            throw new IllegalStateException("Primary decision outbox identity mismatch");
        }
        if (!secondary && decision.secondaryRequired()) {
            if (!"PENDING".equals(status)) throw new IllegalStateException("Secondary outbox has final topic state");
            send(MessageTopics.TCP_SEND, decision.clientMsgId(), SecondarySendCommand.from(decision, submission));
            markPublished(key, item, now, "PENDING", false);
            return;
        }
        if (!secondary) finalized = new FinalizedMessageResult(decision, submission);
        if ("PENDING".equals(status)) {
            send(MessageTopics.MSG_RESULT_FINALIZED, decision.clientMsgId(), finalized);
            markFinalizedPublished(key, item, now);
        }
        send(MessageTopics.WEBHOOK_SEND, decision.clientMsgId(), CustomerWebhookSendCommand.from(finalized));
        markPublished(key, item, now, "FINALIZED_PUBLISHED", true);
    }

    private void send(String topic, String clientMsgId, Object value)
            throws ExecutionException, InterruptedException {
        try (var timing = metrics.start(HANDOFF)) {
            kafka.send(topic, clientMsgId, value).get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    private void markFinalizedPublished(Map<String, AttributeValue> key, Map<String, AttributeValue> item, long now) {
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(STEP).key(key)
                    .conditionExpression("#status = :pending AND result_payload = :payload "
                            + "AND submission_payload = :submission AND #bucket = :bucket AND #due = :due")
                    .updateExpression("SET #status = :next, finalized_published_at_ms = :now")
                    .expressionAttributeNames(Map.of("#status", "status", "#bucket", MessageResultInboxIndex.BUCKET,
                            "#due", MessageResultInboxIndex.DUE))
                    .expressionAttributeValues(Map.of(":pending", s("PENDING"), ":next", s("FINALIZED_PUBLISHED"),
                            ":payload", item.get("result_payload"), ":submission", item.get("submission_payload"),
                            ":bucket", item.get(MessageResultInboxIndex.BUCKET),
                            ":due", item.get(MessageResultInboxIndex.DUE),
                            ":now", AttributeValue.fromN(Long.toString(now)))).build());
        } catch (ConditionalCheckFailedException changed) {
            var latest = read(key);
            String status = latest == null ? "" : latest.getOrDefault("status", s("")).s();
            if (!("FINALIZED_PUBLISHED".equals(status) || "PUBLISHED".equals(status))
                    || !samePayload(item, latest)) throw changed;
        }
    }

    private void markPublished(Map<String, AttributeValue> key, Map<String, AttributeValue> item,
                               long now, String expectedStatus, boolean finalized) {
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(STEP).key(key)
                    .conditionExpression("#status = :expected AND result_payload = :payload "
                            + "AND submission_payload = :submission AND #bucket = :bucket AND #due = :due"
                            + (finalized ? " AND attribute_exists(finalized_published_at_ms)" : ""))
                    .updateExpression("SET #status = :published, published_at_ms = :now REMOVE #bucket, #due")
                    .expressionAttributeNames(Map.of("#status", "status", "#bucket", MessageResultInboxIndex.BUCKET,
                            "#due", MessageResultInboxIndex.DUE))
                    .expressionAttributeValues(Map.of(":expected", s(expectedStatus), ":published", s("PUBLISHED"),
                            ":payload", item.get("result_payload"), ":submission", item.get("submission_payload"),
                            ":bucket", item.get(MessageResultInboxIndex.BUCKET),
                            ":due", item.get(MessageResultInboxIndex.DUE),
                            ":now", AttributeValue.fromN(Long.toString(now)))).build());
        } catch (ConditionalCheckFailedException changed) {
            var latest = read(key);
            if (latest == null || !"PUBLISHED".equals(latest.getOrDefault("status", s("")).s())
                    || !samePayload(item, latest)) throw changed;
        }
    }

    private static boolean samePayload(Map<String, AttributeValue> expected, Map<String, AttributeValue> current) {
        return expected.get("result_payload").equals(current.get("result_payload"))
                && expected.get("submission_payload").equals(current.get("submission_payload"));
    }

    private Map<String, AttributeValue> read(Map<String, AttributeValue> key) {
        return db.getItem(GetItemRequest.builder().tableName(STEP).key(key).consistentRead(true).build()).item();
    }

    private static boolean eligible(Map<String, AttributeValue> candidate) {
        return candidate.get("sk").s().startsWith("PRIMARY_DECISION#")
                || candidate.get("sk").s().startsWith("SECONDARY_DECISION#");
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

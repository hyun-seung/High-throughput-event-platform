package messaging.result;

import messaging.common.messages.MessageResultInboxIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

/** Resolves captured first-send results and leaves their durable next-stage outbox in STEP. */
@Component
public class PendingResultDispatcher {
    private static final Logger log = LoggerFactory.getLogger(PendingResultDispatcher.class);
    private final DynamoDbClient db;
    private final MessageResultInboxStore inbox;
    private final WebhookPrimaryDecisionService decisions;
    private final HttpFailureFollowupService http;
    private final PrimaryStageDecisionStore terminal;
    private final JsonMapper mapper;
    private final Clock clock;
    private final int pageSize;
    private final Map<Integer, Map<String, AttributeValue>> cursors = new HashMap<>();

    public PendingResultDispatcher(DynamoDbClient db, MessageResultInboxStore inbox,
                                    WebhookPrimaryDecisionService decisions,
                                    HttpFailureFollowupService http,
                                    PrimaryStageDecisionStore terminal, JsonMapper mapper, Clock clock,
                                    @org.springframework.beans.factory.annotation.Value(
                                            "${messaging.result.webhook.page-size:100}") int pageSize) {
        this.db = Objects.requireNonNull(db);
        this.inbox = Objects.requireNonNull(inbox);
        this.decisions = Objects.requireNonNull(decisions);
        this.http = Objects.requireNonNull(http);
        this.terminal = Objects.requireNonNull(terminal);
        this.mapper = Objects.requireNonNull(mapper);
        this.clock = Objects.requireNonNull(clock);
        if (pageSize < 1 || pageSize > 1000) throw new IllegalArgumentException("Invalid webhook page size");
        this.pageSize = pageSize;
    }

    @Scheduled(initialDelayString = "${messaging.result.webhook.initial-delay-ms:5000}",
            fixedDelayString = "${messaging.result.webhook.poll-ms:5000}")
    public synchronized void poll() {
        Instant now = clock.instant();
        for (int shard = 0; shard < MessageResultInboxIndex.SHARDS; shard++) pollShard(shard, now);
    }

    private void pollShard(int shard, Instant now) {
        var page = db.query(QueryRequest.builder().tableName(STEP).indexName(MessageResultInboxIndex.NAME)
                .keyConditionExpression("#bucket = :bucket AND #due <= :now")
                .expressionAttributeNames(Map.of("#bucket", MessageResultInboxIndex.BUCKET,
                        "#due", MessageResultInboxIndex.DUE))
                .expressionAttributeValues(Map.of(":bucket", s("message-result-v1-" + shard),
                        ":now", AttributeValue.fromN(Long.toString(now.toEpochMilli()))))
                .exclusiveStartKey(cursors.getOrDefault(shard, Map.of())).limit(pageSize).build());
        for (var candidate : page.items()) {
            try {
                dispatch(Map.of("pk", candidate.get("pk"), "sk", candidate.get("sk")), now);
            } catch (Exception failure) {
                log.error("Pending first-send result remains due: pk={}, sk={}", candidate.get("pk"), candidate.get("sk"), failure);
            }
        }
        if (page.lastEvaluatedKey().isEmpty()) cursors.remove(shard);
        else cursors.put(shard, page.lastEvaluatedKey());
    }

    void dispatch(Map<String, AttributeValue> key, Instant now) {
        if (!key.get("sk").s().startsWith("RESULT_INBOX#")) return;
        var item = inbox.loadPending(key, now);
        if (item == null) return;
        if ("WEBHOOK".equals(item.source())) {
            var outcome = decisions.process(item);
            if (outcome instanceof WebhookPrimaryDecisionService.FollowupStored
                    || outcome instanceof WebhookPrimaryDecisionService.Ignored) {
                inbox.processed(item);
            } else if (outcome instanceof WebhookPrimaryDecisionService.AwaitingHttp) {
                inbox.defer(item, now.plus(Duration.ofSeconds(5)));
            } else if (outcome instanceof WebhookPrimaryDecisionService.SuccessPending success) {
                terminal.fromWebhook(item, success.previousDecisionId(), success.command(), null);
                inbox.processed(item);
            } else if (outcome instanceof WebhookPrimaryDecisionService.PrimaryFailurePending failure) {
                terminal.fromWebhook(item, failure.previousDecisionId(), failure.command(), failure.errorCode());
                inbox.processed(item);
            }
        } else if ("HTTP_RESPONSE".equals(item.source()) || "HTTP_TIMEOUT".equals(item.source())) {
            var result = mapper.readValue(
                    item.payload(), messaging.common.messages.CarrierHttpResult.class);
            var outcome = http.process(result);
            if (outcome instanceof HttpFailureFollowupService.PrimaryFailurePending failure) {
                terminal.fromHttp(item, failure);
            }
            inbox.processed(item);
        } else if ("PRE_SEND".equals(item.source())) {
            terminal.fromPreSend(item);
            inbox.processed(item);
        }
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

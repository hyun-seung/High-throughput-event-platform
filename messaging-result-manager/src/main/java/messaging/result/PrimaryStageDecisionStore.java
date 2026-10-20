package messaging.result;

import messaging.common.messages.FollowupHttpCommand;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.PreSendDispatch;
import messaging.common.messages.PreSendFailure;
import messaging.common.messages.PrimaryExpiryIndex;
import messaging.common.messages.PrimaryStageDecision;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;
import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

/** Freezes first-send business state, then directly publishes its next stage to Kafka. */
public class PrimaryStageDecisionStore {
    public enum Outcome { STORED, ALREADY_CLOSED, WAITING }

    private final DynamoDbClient db;
    private final JsonMapper mapper;
    private final Clock clock;
    private final ResultKafkaPublisher publisher;

    public PrimaryStageDecisionStore(DynamoDbClient db, JsonMapper mapper, Clock clock, ResultKafkaPublisher publisher) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
        this.clock = Objects.requireNonNull(clock);
        this.publisher = Objects.requireNonNull(publisher);
    }

    /** Resumes the canonical frozen result when a redelivery encounters a closed stage. */
    public void resume(String clientMsgId) {
        publisher.publish(read(ORIGIN, MessageOriginCodec.key(clientMsgId)));
    }

    public Outcome fromWebhook(MessageResultInboxStore.Item item, String previousDecisionId,
                               HttpSendCommand command, Integer errorCode, Map<String, AttributeValue> origin) {
        if (!"WEBHOOK".equals(item.source())) throw new IllegalArgumentException("Expected webhook item");
        var webhook = mapper.readValue(item.payload(), MessageResultInboxStore.WebhookItem.class);
        if (!item.clientMsgId().equals(webhook.result().clientMsgId())
                || !item.resultId().equals(MessageResultInboxStore.webhookResultId(
                webhook.traceId(), item.clientMsgId()))
                || command.carrier() != webhook.carrier()
                || ("success".equals(webhook.result().status())) != (errorCode == null)) {
            throw new IllegalArgumentException("Webhook decision context mismatch");
        }
        if (!item.clientMsgId().equals(Objects.requireNonNull(origin).getOrDefault("delivery_id", s("")).s())) {
            throw new IllegalArgumentException("Webhook ORIGIN belongs to another message");
        }
        return freeze(item, previousDecisionId, command, errorCode,
                null, errorCode == null ? PrimaryStageDecision.Kind.SUCCESS : PrimaryStageDecision.Kind.FAILURE, origin);
    }

    public Outcome fromHttp(MessageResultInboxStore.Item item,
                            HttpFailureFollowupService.PrimaryFailurePending failure) {
        if (!"HTTP_RESPONSE".equals(item.source()) && !"HTTP_TIMEOUT".equals(item.source())) {
            throw new IllegalArgumentException("Expected HTTP result item");
        }
        var result = mapper.readValue(item.payload(), messaging.common.messages.CarrierHttpResult.class);
        if (!item.clientMsgId().equals(result.clientMsgId()) || !item.resultId().equals(result.resultId())
                || !failure.command().attemptId().equals(result.attemptId())
                || failure.command().carrier() != result.carrier()
                || failure.command().invocation() != result.invocation()) {
            throw new IllegalArgumentException("HTTP decision context mismatch");
        }
        return freeze(item, failure.previousDecisionId(), failure.command(), failure.errorCode(),
                null, PrimaryStageDecision.Kind.FAILURE);
    }

    public Outcome fromPreSend(MessageResultInboxStore.Item item) {
        if (!"PRE_SEND".equals(item.source())) throw new IllegalArgumentException("Expected pre-send item");
        var failure = mapper.readValue(item.payload(), PreSendFailure.class);
        if (!item.clientMsgId().equals(failure.clientMsgId()) || !item.resultId().equals(failure.resultId())) {
            throw new IllegalArgumentException("Pre-send decision context mismatch");
        }
        return freeze(item, null, null, null, failure.reason().name(), PrimaryStageDecision.Kind.FAILURE);
    }

    public Outcome fromExpiry(String clientMsgId) {
        var origin = read(ORIGIN, MessageOriginCodec.key(clientMsgId));
        if (origin == null || !origin.containsKey(MessageOriginCodec.MESSAGE_ID)
                || !MessageOriginCodec.STATUS_RECEIVED.equals(origin.getOrDefault("status", s("")).s())) {
            publisher.publish(origin);
            return Outcome.ALREADY_CLOSED;
        }
        var deadline = origin.get(PrimaryExpiryIndex.DEADLINE);
        if (deadline == null || clock.millis() < Long.parseLong(deadline.n())) return Outcome.WAITING;
        if (hasEarlierPendingResult(clientMsgId, Long.parseLong(deadline.n()))) return Outcome.WAITING;
        var dispatch = origin.containsKey("pre_send_dispatch")
                ? mapper.readValue(origin.get("pre_send_dispatch").s(), PreSendDispatch.class) : null;
        String previous = origin.containsKey(FollowupHttpCommand.CURRENT_DECISION)
                ? origin.get(FollowupHttpCommand.CURRENT_DECISION).s() : null;
        HttpSendCommand current = previous == null ? dispatch == null ? null : dispatch.command()
                : mapper.readValue(origin.get(FollowupHttpCommand.CURRENT_COMMAND).s(), HttpSendCommand.class);
        String id = UUID.nameUUIDFromBytes(("primary-expiry:" + clientMsgId)
                .getBytes(StandardCharsets.UTF_8)).toString();
        var expired = new MessageResultInboxStore.Item(clientMsgId, id, "PRIMARY_EXPIRY", "{}",
                Instant.ofEpochMilli(Long.parseLong(deadline.n())));
        return freeze(expired, previous, current, null, "PRIMARY_EXPIRED", PrimaryStageDecision.Kind.FAILURE);
    }

    public void deferExpiry(String clientMsgId, Instant nextCheck) {
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(ORIGIN).key(MessageOriginCodec.key(clientMsgId))
                    .conditionExpression("#status = :received AND attribute_exists(#bucket) AND #due <= :now")
                    .updateExpression("SET #due = :next")
                    .expressionAttributeNames(Map.of("#status", "status", "#bucket", PrimaryExpiryIndex.BUCKET,
                            "#due", PrimaryExpiryIndex.DUE))
                    .expressionAttributeValues(Map.of(":received", s(MessageOriginCodec.STATUS_RECEIVED),
                            ":now", AttributeValue.fromN(Long.toString(clock.millis())),
                            ":next", AttributeValue.fromN(Long.toString(nextCheck.toEpochMilli())))).build());
        } catch (ConditionalCheckFailedException changed) {
            var current = read(ORIGIN, MessageOriginCodec.key(clientMsgId));
            if (current != null && MessageOriginCodec.STATUS_RECEIVED.equals(
                    current.getOrDefault("status", s("")).s())) throw changed;
        }
    }

    private boolean hasEarlierPendingResult(String clientMsgId, long deadlineMs) {
        Map<String, AttributeValue> cursor = Map.of();
        do {
            var page = db.query(QueryRequest.builder().tableName(STEP).consistentRead(true)
                    .keyConditionExpression("pk = :pk AND begins_with(sk, :prefix)")
                    .expressionAttributeValues(Map.of(":pk", s("DELIVERY#" + clientMsgId),
                            ":prefix", s("RESULT_INBOX#")))
                    .exclusiveStartKey(cursor.isEmpty() ? null : cursor).build());
            for (var item : page.items()) {
                if ("PENDING".equals(item.getOrDefault("status", s("")).s())
                        && Instant.parse(item.get("received_at").s()).toEpochMilli() <= deadlineMs) return true;
            }
            cursor = page.lastEvaluatedKey();
        } while (!cursor.isEmpty());
        return false;
    }

    private Outcome freeze(MessageResultInboxStore.Item item, String previousDecisionId,
                           HttpSendCommand command, Integer errorCode, String reason,
                           PrimaryStageDecision.Kind kind) {
        return freeze(item, previousDecisionId, command, errorCode, reason, kind,
                read(ORIGIN, MessageOriginCodec.key(item.clientMsgId())));
    }

    private Outcome freeze(MessageResultInboxStore.Item item, String previousDecisionId,
                           HttpSendCommand command, Integer errorCode, String reason,
                           PrimaryStageDecision.Kind kind, Map<String, AttributeValue> origin) {
        // Webhooks reuse their verified snapshot. The conditional write still checks
        // current state/invocation; a race or ambiguous write response reloads below.
        if (!("PRIMARY_EXPIRY".equals(item.source()) ? activeForExpiry(origin) : active(origin))) {
            publisher.publish(origin);
            return Outcome.ALREADY_CLOSED;
        }
        boolean secondary = kind == PrimaryStageDecision.Kind.FAILURE
                && origin.containsKey("secondary_send_payload");
        var decision = new PrimaryStageDecision(item.resultId(), item.clientMsgId(), kind, item.source(),
                errorCode, reason, command == null ? null : command.carrier(),
                command == null ? null : command.invocation(), secondary, clock.instant());
        String encoded = mapper.writeValueAsString(decision);
        String nextStatus = kind == PrimaryStageDecision.Kind.SUCCESS ? "PRIMARY_SUCCEEDED"
                : secondary ? "SECONDARY_PENDING"
                : "PRIMARY_EXPIRED".equals(reason) ? "PRIMARY_EXPIRED" : "PRIMARY_FAILED";
        var values = new HashMap<String, AttributeValue>();
        var names = new HashMap<String, String>();
        names.put("#status", "status");
        names.put("#currentDecision", FollowupHttpCommand.CURRENT_DECISION);
        values.put(":id", s(item.clientMsgId()));
        values.put(":received", s(MessageOriginCodec.STATUS_RECEIVED));
        values.put(":next", s(nextStatus));
        values.put(":decisionId", s(item.resultId()));
        values.put(":decision", s(encoded));
        values.put(":now", s(decision.decidedAt().toString()));
        String condition;
        if ("PRIMARY_EXPIRY".equals(item.source()) && command == null) {
            condition = origin.containsKey("pre_send_dispatch")
                    ? "pre_send_dispatch = :dispatch AND attribute_not_exists(#currentDecision)"
                    : "attribute_not_exists(pre_send_dispatch) AND attribute_not_exists(#currentDecision)";
            if (origin.containsKey("pre_send_dispatch")) values.put(":dispatch", origin.get("pre_send_dispatch"));
        } else if (command == null) {
            var failure = mapper.readValue(item.payload(), PreSendFailure.class);
            condition = "pre_send_dispatch = :dispatch AND attribute_not_exists(#currentDecision)";
            values.put(":dispatch", s(mapper.writeValueAsString(new PreSendDispatch(null, failure))));
        } else if (previousDecisionId == null) {
            condition = "pre_send_dispatch = :dispatch AND attribute_not_exists(#currentDecision)";
            values.put(":dispatch", s(mapper.writeValueAsString(new PreSendDispatch(command, null))));
        } else {
            condition = "#currentDecision = :previous AND #currentCommand = :command";
            names.put("#currentCommand", FollowupHttpCommand.CURRENT_COMMAND);
            values.put(":previous", s(previousDecisionId));
            values.put(":command", s(mapper.writeValueAsString(command)));
        }
        if (command == null && !"PRIMARY_EXPIRY".equals(item.source())
                && !mapper.writeValueAsString(new PreSendDispatch(null,
                mapper.readValue(item.payload(), PreSendFailure.class)))
                .equals(origin.getOrDefault("pre_send_dispatch", s("")).s())) return Outcome.ALREADY_CLOSED;
        if (command != null && !command.request().clientMsgId().equals(item.clientMsgId())) {
            throw new IllegalArgumentException("Primary decision command belongs to another message");
        }
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(ORIGIN)
                    .key(MessageOriginCodec.key(item.clientMsgId()))
                    .conditionExpression("delivery_id = :id AND #status = :received "
                            + "AND attribute_not_exists(primary_decision_id) "
                            + "AND attribute_not_exists(completion_event_id) AND " + condition)
                    .updateExpression("SET #status = :next, primary_decision_id = :decisionId, "
                            + "primary_decision = :decision, updated_at = :now")
                    .expressionAttributeNames(names).expressionAttributeValues(values).build());
        } catch (RuntimeException changed) {
            // A write may have committed even if its response was lost. Use the stored
            // decision (including its original timestamp), never manufacture a new result.
            var latest = read(ORIGIN, MessageOriginCodec.key(item.clientMsgId()));
            if (latest != null && latest.containsKey("primary_decision")) {
                publisher.publish(latest);
                return item.resultId().equals(latest.get("primary_decision_id").s())
                        ? Outcome.STORED : Outcome.ALREADY_CLOSED;
            }
            if (changed instanceof ConditionalCheckFailedException && !activeForExpiry(latest)) {
                return Outcome.ALREADY_CLOSED;
            }
            throw changed;
        }
        var frozen = new HashMap<>(origin);
        frozen.put("status", s(nextStatus));
        frozen.put("primary_decision_id", s(item.resultId()));
        frozen.put("primary_decision", s(encoded));
        publisher.publish(frozen);
        return Outcome.STORED;
    }

    private Map<String, AttributeValue> read(String table, Map<String, AttributeValue> key) {
        return db.getItem(GetItemRequest.builder().tableName(table).key(key).consistentRead(true).build()).item();
    }

    private static boolean active(Map<String, AttributeValue> origin) {
        return activeForExpiry(origin) && origin.containsKey("pre_send_dispatch");
    }

    private static boolean activeForExpiry(Map<String, AttributeValue> origin) {
        return origin != null && origin.containsKey(MessageOriginCodec.MESSAGE_ID)
                && !origin.containsKey("completion_event_id")
                && MessageOriginCodec.STATUS_RECEIVED.equals(origin.getOrDefault("status", s("")).s());
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

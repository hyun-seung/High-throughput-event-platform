package messaging.result;

import messaging.common.messages.FollowupHttpCommand;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageResultInboxIndex;
import messaging.common.messages.PreSendDispatch;
import messaging.common.messages.PreSendFailure;
import messaging.common.messages.PrimaryStageDecision;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;
import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

/** Atomically closes the first-send stage and preserves its next-stage handoff. */
public class PrimaryStageDecisionStore {
    public enum Outcome { STORED, ALREADY_CLOSED }

    private final DynamoDbClient db;
    private final JsonMapper mapper;
    private final Clock clock;

    public PrimaryStageDecisionStore(DynamoDbClient db, JsonMapper mapper, Clock clock) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
        this.clock = Objects.requireNonNull(clock);
    }

    public Outcome fromWebhook(MessageResultInboxStore.Item item, String previousDecisionId,
                               HttpSendCommand command, Integer errorCode) {
        if (!"WEBHOOK".equals(item.source())) throw new IllegalArgumentException("Expected webhook item");
        var webhook = mapper.readValue(item.payload(), MessageResultInboxStore.WebhookItem.class);
        if (!item.clientMsgId().equals(webhook.result().clientMsgId())
                || !item.resultId().equals(MessageResultInboxStore.webhookResultId(
                webhook.traceId(), item.clientMsgId()))
                || command.carrier() != webhook.carrier()
                || ("success".equals(webhook.result().status())) != (errorCode == null)) {
            throw new IllegalArgumentException("Webhook decision context mismatch");
        }
        return freeze(item, previousDecisionId, command, errorCode,
                null, errorCode == null ? PrimaryStageDecision.Kind.SUCCESS : PrimaryStageDecision.Kind.FAILURE);
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

    private Outcome freeze(MessageResultInboxStore.Item item, String previousDecisionId,
                           HttpSendCommand command, Integer errorCode, String reason,
                           PrimaryStageDecision.Kind kind) {
        var origin = read(ORIGIN, MessageOriginCodec.key(item.clientMsgId()));
        if (!active(origin)) return Outcome.ALREADY_CLOSED;
        boolean secondary = kind == PrimaryStageDecision.Kind.FAILURE
                && origin.containsKey("secondary_send_payload");
        var decision = new PrimaryStageDecision(item.resultId(), item.clientMsgId(), kind, item.source(),
                errorCode, reason, command == null ? null : command.carrier(),
                command == null ? null : command.invocation(), secondary, clock.instant());
        String encoded = mapper.writeValueAsString(decision);
        String nextStatus = kind == PrimaryStageDecision.Kind.SUCCESS ? "PRIMARY_SUCCEEDED"
                : secondary ? "SECONDARY_PENDING" : "PRIMARY_FAILED";
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
        if (command == null) {
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
        if (command == null && !mapper.writeValueAsString(new PreSendDispatch(null,
                mapper.readValue(item.payload(), PreSendFailure.class)))
                .equals(origin.getOrDefault("pre_send_dispatch", s("")).s())) return Outcome.ALREADY_CLOSED;
        if (command != null && !command.request().clientMsgId().equals(item.clientMsgId())) {
            throw new IllegalArgumentException("Primary decision command belongs to another message");
        }
        var outbox = new HashMap<String, AttributeValue>();
        outbox.put("pk", s("DELIVERY#" + item.clientMsgId()));
        outbox.put("sk", s("PRIMARY_DECISION#" + item.resultId()));
        outbox.put("schema_version", AttributeValue.fromN("4"));
        outbox.put("delivery_id", s(item.clientMsgId()));
        outbox.put("result_id", s(item.resultId()));
        outbox.put("source", s("PRIMARY_DECISION"));
        outbox.put("result_payload", s(encoded));
        outbox.put("status", s("PENDING"));
        outbox.put("received_at", s(decision.decidedAt().toString()));
        MessageResultInboxIndex.add(outbox, item.clientMsgId(), decision.decidedAt().toEpochMilli());
        try {
            db.transactWriteItems(builder -> builder.transactItems(
                    TransactWriteItem.builder().update(Update.builder().tableName(ORIGIN)
                            .key(MessageOriginCodec.key(item.clientMsgId()))
                            .conditionExpression("delivery_id = :id AND #status = :received "
                                    + "AND attribute_not_exists(primary_decision_id) "
                                    + "AND attribute_not_exists(completion_event_id) AND " + condition)
                            .updateExpression("SET #status = :next, primary_decision_id = :decisionId, "
                                    + "primary_decision = :decision, updated_at = :now")
                            .expressionAttributeNames(names)
                            .expressionAttributeValues(values).build()).build(),
                    TransactWriteItem.builder().put(Put.builder().tableName(STEP).item(outbox)
                            .conditionExpression("attribute_not_exists(pk)").build()).build()));
            return Outcome.STORED;
        } catch (TransactionCanceledException changed) {
            if (!changed.cancellationReasons().stream().anyMatch(
                    reasonCode -> "ConditionalCheckFailed".equals(reasonCode.code()))) throw changed;
            var latest = read(ORIGIN, MessageOriginCodec.key(item.clientMsgId()));
            var stored = read(STEP, Map.of("pk", outbox.get("pk"), "sk", outbox.get("sk")));
            if (item.resultId().equals(latest.getOrDefault("primary_decision_id", s("")).s())
                    && encoded.equals(stored.getOrDefault("result_payload", s("")).s())) return Outcome.STORED;
            if (!active(latest)) return Outcome.ALREADY_CLOSED;
            throw new IllegalStateException("Primary decision conflicts with active state", changed);
        }
    }

    private Map<String, AttributeValue> read(String table, Map<String, AttributeValue> key) {
        return db.getItem(GetItemRequest.builder().tableName(table).key(key).consistentRead(true).build()).item();
    }

    private static boolean active(Map<String, AttributeValue> origin) {
        return origin != null && origin.containsKey(MessageOriginCodec.MESSAGE_ID)
                && origin.containsKey("pre_send_dispatch") && !origin.containsKey("completion_event_id")
                && MessageOriginCodec.STATUS_RECEIVED.equals(origin.getOrDefault("status", s("")).s());
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

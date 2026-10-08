package messaging.result;

import messaging.common.messages.CarrierHttpResult;
import messaging.common.messages.FollowupHttpCommand;
import messaging.common.messages.HttpCarrier;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.PreSendDispatch;
import messaging.common.messages.PrimaryHttpFailureDecision;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;
import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

/** Applies a webhook only to the active carrier invocation, preserving undecided results. */
public class WebhookPrimaryDecisionService {
    public sealed interface Outcome permits FollowupStored, SuccessPending, PrimaryFailurePending,
            AwaitingHttp, Ignored { }
    public record FollowupStored(FollowupHttpCommand value) implements Outcome { }
    public record SuccessPending(String previousDecisionId, HttpSendCommand command) implements Outcome { }
    public record PrimaryFailurePending(int errorCode, String previousDecisionId,
                                        HttpSendCommand command) implements Outcome { }
    public record AwaitingHttp() implements Outcome { }
    public record Ignored() implements Outcome { }

    private final DynamoDbClient db;
    private final JsonMapper mapper;
    private final FollowupHttpCommandStore commands;
    private final Clock clock;

    public WebhookPrimaryDecisionService(DynamoDbClient db, JsonMapper mapper,
                                         FollowupHttpCommandStore commands, Clock clock) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
        this.commands = Objects.requireNonNull(commands);
        this.clock = Objects.requireNonNull(clock);
    }

    public Outcome process(MessageResultInboxStore.Item item) {
        Objects.requireNonNull(item);
        if (!"WEBHOOK".equals(item.source())) throw new IllegalArgumentException("Not a webhook inbox item");
        var webhook = mapper.readValue(item.payload(), MessageResultInboxStore.WebhookItem.class);
        if (!item.clientMsgId().equals(webhook.result().clientMsgId())
                || !item.resultId().equals(MessageResultInboxStore.webhookResultId(
                webhook.traceId(), item.clientMsgId()))) {
            throw new IllegalArgumentException("Webhook inbox identity mismatch");
        }
        var origin = read(ORIGIN, MessageOriginCodec.key(item.clientMsgId()));
        if (!active(origin)) return new Ignored();
        var initial = mapper.readValue(origin.get("pre_send_dispatch").s(), PreSendDispatch.class);
        if (initial.command() == null) return new Ignored();
        String previousDecision = origin.containsKey(FollowupHttpCommand.CURRENT_DECISION)
                ? origin.get(FollowupHttpCommand.CURRENT_DECISION).s() : null;
        if (item.resultId().equals(previousDecision) || alreadyAuthorized(item.clientMsgId(), item.resultId())) {
            return new Ignored();
        }
        HttpSendCommand current = previousDecision == null ? initial.command()
                : mapper.readValue(origin.get(FollowupHttpCommand.CURRENT_COMMAND).s(), HttpSendCommand.class);
        if (current.carrier() != webhook.carrier()) return new Ignored();

        var step = read(STEP, current.stepKey());
        if (step == null || step.isEmpty() || !"OBSERVED".equals(step.getOrDefault("status", s("")).s())) {
            return new AwaitingHttp();
        }
        if (!mapper.writeValueAsString(current).equals(step.getOrDefault("command", s("")).s())) {
            throw new IllegalStateException("Webhook current HTTP command conflicts with STEP");
        }
        var observation = step.get("http_observation");
        if (observation == null) return new AwaitingHttp();
        var http = mapper.readValue(observation.s(), CarrierHttpResult.class);
        if (http.status() == CarrierHttpResult.Status.FAILED) return new Ignored();

        if ("success".equals(webhook.result().status())) return new SuccessPending(previousDecision, current);
        var action = PrimaryHttpFailureDecision.decide(webhook.result().error().code(),
                current.carrier(), current.invocation(), attempted(item.clientMsgId()), clock.instant());
        if (action instanceof PrimaryHttpFailureDecision.FailPrimary failed) {
            return new PrimaryFailurePending(failed.errorCode(), previousDecision, current);
        }
        var send = (PrimaryHttpFailureDecision.Send) action;
        var next = new HttpSendCommand(HttpSendCommand.attemptId(item.clientMsgId(), send.carrier()),
                send.carrier(), send.invocation(), current.deadlineAt(), current.request());
        var followup = new FollowupHttpCommand(item.resultId(), next, send.notBefore());
        return new FollowupStored(commands.freeze(item.clientMsgId(), previousDecision, followup));
    }

    private Set<HttpCarrier> attempted(String clientMsgId) {
        var carriers = EnumSet.noneOf(HttpCarrier.class);
        Map<String, AttributeValue> cursor = Map.of();
        do {
            var page = db.query(QueryRequest.builder().tableName(STEP).consistentRead(true)
                    .keyConditionExpression("pk = :pk AND begins_with(sk, :prefix)")
                    .expressionAttributeValues(Map.of(":pk", s("DELIVERY#" + clientMsgId),
                            ":prefix", s("HTTP#")))
                    .exclusiveStartKey(cursor.isEmpty() ? null : cursor).build());
            for (var recorded : page.items()) {
                if ("OBSERVED".equals(recorded.getOrDefault("status", s("")).s())) {
                    carriers.add(HttpCarrier.valueOf(recorded.get("carrier").s()));
                }
            }
            cursor = page.lastEvaluatedKey();
        } while (!cursor.isEmpty());
        return carriers;
    }

    private boolean alreadyAuthorized(String clientMsgId, String resultId) {
        Map<String, AttributeValue> cursor = Map.of();
        do {
            var page = db.query(QueryRequest.builder().tableName(STEP).consistentRead(true)
                    .keyConditionExpression("pk = :pk AND begins_with(sk, :prefix)")
                    .expressionAttributeValues(Map.of(":pk", s("DELIVERY#" + clientMsgId),
                            ":prefix", s("HTTP_COMMAND#")))
                    .exclusiveStartKey(cursor.isEmpty() ? null : cursor).build());
            for (var recorded : page.items()) {
                if (resultId.equals(recorded.getOrDefault("decision_id", s("")).s())) return true;
            }
            cursor = page.lastEvaluatedKey();
        } while (!cursor.isEmpty());
        return false;
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

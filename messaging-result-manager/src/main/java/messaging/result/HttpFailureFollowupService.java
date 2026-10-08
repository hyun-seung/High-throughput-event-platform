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

/** Verifies the current HTTP observation and freezes its next command when one is allowed. */
public class HttpFailureFollowupService {
    public sealed interface Outcome permits FollowupStored, PrimaryFailurePending, Ignored { }
    public record FollowupStored(FollowupHttpCommand value) implements Outcome { }
    /** The first-send failure still needs secondary/finalization processing. */
    public record PrimaryFailurePending(int errorCode, String previousDecisionId,
                                        HttpSendCommand command) implements Outcome { }
    public record Ignored() implements Outcome { }

    private final DynamoDbClient db;
    private final JsonMapper mapper;
    private final FollowupHttpCommandStore commands;
    private final Clock clock;

    public HttpFailureFollowupService(DynamoDbClient db, JsonMapper mapper,
                                      FollowupHttpCommandStore commands, Clock clock) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
        this.commands = Objects.requireNonNull(commands);
        this.clock = Objects.requireNonNull(clock);
    }

    public Outcome process(CarrierHttpResult result) {
        Objects.requireNonNull(result);
        if (!result.needsPublication() || !CarrierHttpResult.id(result.clientMsgId(), result.attemptId(),
                result.invocation()).equals(result.resultId())) {
            throw new IllegalArgumentException("Invalid HTTP failure result identity");
        }
        var origin = read(ORIGIN, MessageOriginCodec.key(result.clientMsgId()));
        if (!active(origin) || !result.clientMsgId().equals(origin.getOrDefault("delivery_id", s("")).s())) {
            return new Ignored();
        }
        PreSendDispatch initial = mapper.readValue(origin.get("pre_send_dispatch").s(), PreSendDispatch.class);
        if (initial.command() == null) return new Ignored();
        String previousDecision = origin.containsKey(FollowupHttpCommand.CURRENT_DECISION)
                ? origin.get(FollowupHttpCommand.CURRENT_DECISION).s() : null;
        HttpSendCommand current = previousDecision == null ? initial.command()
                : currentFollowupCommand(origin);
        if (!matches(result, current)) return new Ignored();
        verifyObserved(current, result);

        var now = clock.instant();
        PrimaryHttpFailureDecision.Action action = result.status() == CarrierHttpResult.Status.TIMEOUT
                ? PrimaryHttpFailureDecision.decideNoResponse(result.carrier(), result.invocation(), now)
                : PrimaryHttpFailureDecision.decide(result.normalizedErrorCode(), result.carrier(),
                result.invocation(), attempted(result.clientMsgId()), now);
        if (action instanceof PrimaryHttpFailureDecision.FailPrimary failure) {
            return new PrimaryFailurePending(failure.errorCode(), previousDecision, current);
        }
        var send = (PrimaryHttpFailureDecision.Send) action;
        var next = new HttpSendCommand(HttpSendCommand.attemptId(result.clientMsgId(), send.carrier()),
                send.carrier(), send.invocation(), current.deadlineAt(), current.request());
        var followup = new FollowupHttpCommand(result.resultId(), next, send.notBefore());
        return new FollowupStored(commands.freeze(result.clientMsgId(), previousDecision, followup));
    }

    private HttpSendCommand currentFollowupCommand(Map<String, AttributeValue> origin) {
        var encoded = origin.get(FollowupHttpCommand.CURRENT_COMMAND);
        if (encoded == null) throw new IllegalStateException("Current HTTP command is missing from ORIGIN");
        return mapper.readValue(encoded.s(), HttpSendCommand.class);
    }

    private void verifyObserved(HttpSendCommand command, CarrierHttpResult result) {
        var step = read(STEP, command.stepKey());
        if (step == null || !"OBSERVED".equals(step.getOrDefault("status", s("")).s())
                || !mapper.writeValueAsString(command).equals(step.getOrDefault("command", s("")).s())
                || !mapper.writeValueAsString(result).equals(step.getOrDefault("http_observation", s("")).s())) {
            throw new IllegalStateException("MSG_RESULT does not match a frozen HTTP observation");
        }
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
            for (var item : page.items()) {
                if ("OBSERVED".equals(item.getOrDefault("status", s("")).s())) {
                    carriers.add(HttpCarrier.valueOf(item.get("carrier").s()));
                }
            }
            cursor = page.lastEvaluatedKey();
        } while (!cursor.isEmpty());
        return carriers;
    }

    private static boolean matches(CarrierHttpResult result, HttpSendCommand command) {
        return result.clientMsgId().equals(command.request().clientMsgId())
                && result.attemptId().equals(command.attemptId())
                && result.carrier() == command.carrier() && result.invocation() == command.invocation();
    }

    private static boolean active(Map<String, AttributeValue> origin) {
        return origin != null && origin.containsKey(MessageOriginCodec.MESSAGE_ID)
                && origin.containsKey("pre_send_dispatch") && !origin.containsKey("completion_event_id")
                && MessageOriginCodec.STATUS_RECEIVED.equals(origin.getOrDefault("status", s("")).s());
    }

    private Map<String, AttributeValue> read(String table, Map<String, AttributeValue> key) {
        return db.getItem(GetItemRequest.builder().tableName(table).key(key).consistentRead(true).build()).item();
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

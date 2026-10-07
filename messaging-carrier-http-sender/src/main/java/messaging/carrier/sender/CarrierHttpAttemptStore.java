package messaging.carrier.sender;

import messaging.common.messages.CarrierHttpResult;
import messaging.common.messages.FollowupHttpCommand;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.PreSendDispatch;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;
import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

/** The durable boundary before an HTTP provider call; SENDING is never automatically reclaimed. */
public class CarrierHttpAttemptStore {
    public enum State { PENDING, SENDING, OBSERVED, INELIGIBLE }

    private final DynamoDbClient db;
    private final JsonMapper mapper;

    public CarrierHttpAttemptStore(DynamoDbClient db, JsonMapper mapper) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
    }

    public State reserve(HttpSendCommand command, Instant now) {
        Objects.requireNonNull(command);
        Objects.requireNonNull(now);
        var origin = read(ORIGIN, MessageOriginCodec.key(command.request().clientMsgId()));
        if (!eligible(origin)) return State.INELIGIBLE;
        Authorization authorization = authorize(command, origin, now);
        if (!authorization.current()) return State.INELIGIBLE;

        var item = new HashMap<>(key(command));
        item.put("schema_version", AttributeValue.fromN("4"));
        item.put("delivery_id", s(command.request().clientMsgId()));
        item.put("attempt_id", s(command.attemptId()));
        item.put("carrier", s(command.carrier().name()));
        item.put("invocation", AttributeValue.fromN(Integer.toString(command.invocation())));
        item.put("command", s(mapper.writeValueAsString(command)));
        item.put("status", s(State.PENDING.name()));
        item.put("created_at", s(now.toString()));
        try {
            var writes = checks(command, authorization);
            writes.add(TransactWriteItem.builder().put(Put.builder().tableName(STEP).item(item)
                    .conditionExpression("attribute_not_exists(pk)").build()).build());
            db.transactWriteItems(builder -> builder.transactItems(writes));
            return State.PENDING;
        } catch (TransactionCanceledException collision) {
            if (!conditional(collision)) throw collision;
            var current = read(ORIGIN, MessageOriginCodec.key(command.request().clientMsgId()));
            if (!eligible(current) || !authorize(command, current, now).current()) return State.INELIGIBLE;
            var existing = read(STEP, key(command));
            if (existing.isEmpty()) return State.INELIGIBLE;
            return verifiedState(command, existing);
        }
    }

    public boolean begin(HttpSendCommand command, Instant now) {
        Objects.requireNonNull(command);
        Objects.requireNonNull(now);
        var origin = read(ORIGIN, MessageOriginCodec.key(command.request().clientMsgId()));
        if (!eligible(origin)) return false;
        Authorization authorization = authorize(command, origin, now);
        if (!authorization.current()) return false;
        try {
            var writes = checks(command, authorization);
            writes.add(TransactWriteItem.builder().update(Update.builder().tableName(STEP).key(key(command))
                            .conditionExpression("#status = :pending AND command = :command")
                            .updateExpression("SET #status = :sending, started_at = :started, started_at_ms = :started_ms")
                            .expressionAttributeNames(Map.of("#status", "status"))
                            .expressionAttributeValues(Map.of(
                                    ":pending", s(State.PENDING.name()),
                                    ":command", s(mapper.writeValueAsString(command)),
                                    ":sending", s(State.SENDING.name()),
                                    ":started", s(now.toString()),
                                    ":started_ms", AttributeValue.fromN(Long.toString(now.toEpochMilli())))).build()).build());
            db.transactWriteItems(builder -> builder.transactItems(writes));
            return true;
        } catch (TransactionCanceledException changed) {
            if (!conditional(changed)) throw changed;
            var existing = read(STEP, key(command));
            if (!existing.isEmpty()) verifiedState(command, existing);
            return false;
        }
    }

    public State state(HttpSendCommand command) {
        var item = read(STEP, key(command));
        return item.isEmpty() ? State.INELIGIBLE : verifiedState(command, item);
    }

    public Optional<CarrierHttpResult> observation(HttpSendCommand command) {
        var item = read(STEP, key(command));
        if (item.isEmpty()) return Optional.empty();
        verifiedState(command, item);
        return item.containsKey("http_observation")
                ? Optional.of(mapper.readValue(item.get("http_observation").s(), CarrierHttpResult.class))
                : Optional.empty();
    }

    public CarrierHttpResult record(HttpSendCommand command, CarrierHttpResult result) {
        verifyResult(command, result);
        String encoded = mapper.writeValueAsString(result);
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(STEP).key(key(command))
                    .conditionExpression("#status = :sending AND command = :command "
                            + "AND attribute_not_exists(http_observation)")
                    .updateExpression("SET #status = :observed, http_observation = :observation, "
                            + "publish_state = :publication, updated_at = :now")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(
                            ":sending", s(State.SENDING.name()),
                            ":command", s(mapper.writeValueAsString(command)),
                            ":observed", s(State.OBSERVED.name()),
                            ":observation", s(encoded),
                            ":publication", s(result.needsPublication() ? "PENDING" : "NONE"),
                            ":now", s(result.observedAt().toString()))).build());
            return result;
        } catch (ConditionalCheckFailedException changed) {
            var existing = read(STEP, key(command));
            if (encoded.equals(existing.getOrDefault("http_observation", s("")).s())) return result;
            throw changed;
        }
    }

    /** A crashed in-flight call becomes a timeout observation, never a second direct HTTP call. */
    public Optional<CarrierHttpResult> recoverStale(HttpSendCommand command, Instant now, Instant cutoff) {
        var item = read(STEP, key(command));
        if (item.isEmpty()) return Optional.empty();
        State state = verifiedState(command, item);
        if (state == State.OBSERVED) return observation(command);
        if (state != State.SENDING || !item.containsKey("started_at_ms")
                || Long.parseLong(item.get("started_at_ms").n()) > cutoff.toEpochMilli()) {
            return Optional.empty();
        }
        var timeout = new CarrierHttpResult(CarrierHttpResult.id(command), command.request().clientMsgId(),
                command.attemptId(), command.carrier(), command.invocation(), "HTTP_TIMEOUT",
                CarrierHttpResult.Status.TIMEOUT, null, null, null, null, null, now);
        try {
            return Optional.of(record(command, timeout));
        } catch (ConditionalCheckFailedException competingObservation) {
            return observation(command);
        }
    }

    public void published(HttpSendCommand command, CarrierHttpResult result) {
        verifyResult(command, result);
        if (!result.needsPublication()) return;
        String encoded = mapper.writeValueAsString(result);
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(STEP).key(key(command))
                    .conditionExpression("http_observation = :observation AND publish_state = :pending")
                    .updateExpression("SET publish_state = :published")
                    .expressionAttributeValues(Map.of(
                            ":observation", s(encoded), ":pending", s("PENDING"),
                            ":published", s("PUBLISHED"))).build());
        } catch (ConditionalCheckFailedException changed) {
            var existing = read(STEP, key(command));
            if (!encoded.equals(existing.getOrDefault("http_observation", s("")).s())
                    || !"PUBLISHED".equals(existing.getOrDefault("publish_state", s("")).s())) throw changed;
        }
    }

    private static void verifyResult(HttpSendCommand command, CarrierHttpResult result) {
        if (!CarrierHttpResult.id(command).equals(result.resultId())
                || !command.request().clientMsgId().equals(result.clientMsgId())
                || !command.attemptId().equals(result.attemptId())
                || command.carrier() != result.carrier() || command.invocation() != result.invocation()) {
            throw new IllegalArgumentException("Carrier observation does not match the command");
        }
    }

    private Authorization authorize(HttpSendCommand command, Map<String, AttributeValue> origin, Instant now) {
        PreSendDispatch dispatch = mapper.readValue(origin.get("pre_send_dispatch").s(), PreSendDispatch.class);
        if (command.equals(dispatch.command())) {
            return new Authorization(null, null, !origin.containsKey(FollowupHttpCommand.CURRENT_DECISION));
        }
        if (dispatch.command() == null) {
            throw new IllegalStateException("HTTP command follows a rejected pre-send decision");
        }
        var item = read(STEP, FollowupHttpCommand.key(command));
        if (item == null || !item.containsKey("authorization")) {
            throw new IllegalStateException("HTTP command has no frozen follow-up authorization");
        }
        FollowupHttpCommand followup = mapper.readValue(item.get("authorization").s(), FollowupHttpCommand.class);
        if (!command.equals(followup.command())
                || !followup.decisionId().equals(item.getOrDefault("decision_id", s("")).s())) {
            throw new IllegalStateException("HTTP command does not match frozen authorization");
        }
        if (!followup.decisionId().equals(origin.getOrDefault(FollowupHttpCommand.CURRENT_DECISION, s("")).s())) {
            return new Authorization(null, null, false);
        }
        if (now.isBefore(followup.notBefore())) {
            throw new IllegalStateException("Follow-up HTTP command is not due yet");
        }
        return new Authorization(followup.decisionId(), item.get("authorization").s(), true);
    }

    private List<TransactWriteItem> checks(HttpSendCommand command, Authorization authorization) {
        var writes = new ArrayList<TransactWriteItem>();
        writes.add(originCheck(command, authorization));
        if (authorization.decisionId() != null) {
            writes.add(TransactWriteItem.builder().conditionCheck(ConditionCheck.builder().tableName(STEP)
                    .key(FollowupHttpCommand.key(command))
                    .conditionExpression("decision_id = :decision AND authorization = :authorization")
                    .expressionAttributeValues(Map.of(
                            ":decision", s(authorization.decisionId()),
                            ":authorization", s(authorization.encoded()))).build()).build());
        }
        return writes;
    }

    private TransactWriteItem originCheck(HttpSendCommand command, Authorization authorization) {
        var values = new HashMap<String, AttributeValue>();
        values.put(":execution", s(command.request().clientMsgId()));
        values.put(":received", s(MessageOriginCodec.STATUS_RECEIVED));
        String decisionCondition;
        if (authorization.decisionId() == null) {
            values.put(":dispatch", s(mapper.writeValueAsString(new PreSendDispatch(command, null))));
            decisionCondition = "pre_send_dispatch = :dispatch AND attribute_not_exists(" + FollowupHttpCommand.CURRENT_DECISION + ")";
        } else {
            values.put(":decision", s(authorization.decisionId()));
            decisionCondition = FollowupHttpCommand.CURRENT_DECISION + " = :decision";
        }
        return TransactWriteItem.builder().conditionCheck(ConditionCheck.builder().tableName(ORIGIN)
                .key(MessageOriginCodec.key(command.request().clientMsgId()))
                .conditionExpression("delivery_id = :execution AND #status = :received "
                        + "AND attribute_not_exists(completion_event_id) AND " + decisionCondition)
                .expressionAttributeNames(Map.of("#status", "status"))
                .expressionAttributeValues(values).build()).build();
    }

    private record Authorization(String decisionId, String encoded, boolean current) { }

    private Map<String, AttributeValue> read(String table, Map<String, AttributeValue> key) {
        return db.getItem(GetItemRequest.builder().tableName(table).key(key).consistentRead(true).build()).item();
    }

    private static boolean eligible(Map<String, AttributeValue> item) {
        return item != null && item.containsKey(MessageOriginCodec.MESSAGE_ID)
                && item.containsKey("pre_send_dispatch")
                && !item.containsKey("completion_event_id")
                && MessageOriginCodec.STATUS_RECEIVED.equals(
                        item.getOrDefault("status", s("")).s());
    }

    private State verifiedState(HttpSendCommand command, Map<String, AttributeValue> item) {
        if (!mapper.writeValueAsString(command).equals(item.getOrDefault("command", s("")).s())) {
            throw new IllegalStateException("HTTP STEP command conflict");
        }
        return State.valueOf(item.get("status").s());
    }

    private static boolean conditional(TransactionCanceledException canceled) {
        return canceled.cancellationReasons().stream()
                .anyMatch(reason -> "ConditionalCheckFailed".equals(reason.code()));
    }

    static Map<String, AttributeValue> key(HttpSendCommand command) {
        return command.stepKey();
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

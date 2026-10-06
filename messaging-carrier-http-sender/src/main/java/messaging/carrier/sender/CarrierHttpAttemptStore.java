package messaging.carrier.sender;

import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.PreSendDispatch;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
        PreSendDispatch dispatch = mapper.readValue(origin.get("pre_send_dispatch").s(), PreSendDispatch.class);
        if (!command.equals(dispatch.command())) {
            throw new IllegalStateException("HTTP command does not match frozen ORIGIN dispatch");
        }

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
            db.transactWriteItems(builder -> builder.transactItems(
                    originCheck(command),
                    TransactWriteItem.builder().put(Put.builder().tableName(STEP).item(item)
                            .conditionExpression("attribute_not_exists(pk)").build()).build()));
            return State.PENDING;
        } catch (TransactionCanceledException collision) {
            if (!conditional(collision)) throw collision;
            if (!eligible(read(ORIGIN, MessageOriginCodec.key(command.request().clientMsgId())))) {
                return State.INELIGIBLE;
            }
            var existing = read(STEP, key(command));
            if (existing.isEmpty()) return State.INELIGIBLE;
            return verifiedState(command, existing);
        }
    }

    public boolean begin(HttpSendCommand command, Instant now) {
        Objects.requireNonNull(command);
        Objects.requireNonNull(now);
        try {
            db.transactWriteItems(builder -> builder.transactItems(
                    originCheck(command),
                    TransactWriteItem.builder().update(Update.builder().tableName(STEP).key(key(command))
                            .conditionExpression("#status = :pending AND command = :command")
                            .updateExpression("SET #status = :sending, started_at = :started")
                            .expressionAttributeNames(Map.of("#status", "status"))
                            .expressionAttributeValues(Map.of(
                                    ":pending", s(State.PENDING.name()),
                                    ":command", s(mapper.writeValueAsString(command)),
                                    ":sending", s(State.SENDING.name()),
                                    ":started", s(now.toString()))).build()).build()));
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

    private TransactWriteItem originCheck(HttpSendCommand command) {
        return TransactWriteItem.builder().conditionCheck(ConditionCheck.builder().tableName(ORIGIN)
                .key(MessageOriginCodec.key(command.request().clientMsgId()))
                .conditionExpression("delivery_id = :execution AND #status = :received "
                        + "AND attribute_not_exists(completion_event_id) AND pre_send_dispatch = :dispatch")
                .expressionAttributeNames(Map.of("#status", "status"))
                .expressionAttributeValues(Map.of(
                        ":execution", s(command.request().clientMsgId()),
                        ":received", s(MessageOriginCodec.STATUS_RECEIVED),
                        ":dispatch", s(mapper.writeValueAsString(new PreSendDispatch(command, null))))).build()).build();
    }

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
        return Map.of("pk", s("DELIVERY#" + command.request().clientMsgId()),
                "sk", s("HTTP#" + command.carrier().name() + "#" + command.attemptId()
                        + "#" + command.invocation()));
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

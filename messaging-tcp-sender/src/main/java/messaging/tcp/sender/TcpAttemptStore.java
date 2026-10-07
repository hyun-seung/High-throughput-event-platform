package messaging.tcp.sender;

import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.SecondarySendCommand;
import messaging.common.messages.TcpSendResult;
import org.springframework.stereotype.Component;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;
import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

/** Owns the second-send call fence and its durable result-publication state. */
@Component
public class TcpAttemptStore {
    public enum State { RESERVED, SENDING, OBSERVED, INELIGIBLE }

    private final DynamoDbClient db;
    private final JsonMapper mapper;

    public TcpAttemptStore(DynamoDbClient db, JsonMapper mapper) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
    }

    public State reserve(SecondarySendCommand command, Instant now) {
        var previous = read(STEP, key(command));
        if (previous != null && !previous.isEmpty()) return state(command, previous);
        var origin = read(ORIGIN, MessageOriginCodec.key(command.submission().clientMsgId()));
        if (!eligible(command, origin)) return State.INELIGIBLE;
        String encoded = mapper.writeValueAsString(command);
        var item = new HashMap<>(key(command));
        item.put("schema_version", AttributeValue.fromN("4"));
        item.put("delivery_id", s(command.submission().clientMsgId()));
        item.put("attempt_id", s(command.attemptId()));
        item.put("command", s(encoded));
        item.put("status", s("RESERVED"));
        item.put("created_at", s(now.toString()));
        try {
            db.transactWriteItems(builder -> builder.transactItems(
                    TransactWriteItem.builder().conditionCheck(ConditionCheck.builder().tableName(ORIGIN)
                            .key(MessageOriginCodec.key(command.submission().clientMsgId()))
                            .conditionExpression("#status = :pending AND primary_decision_id = :decision "
                                    + "AND primary_decision = :payload AND attribute_not_exists(completion_event_id)")
                            .expressionAttributeNames(Map.of("#status", "status"))
                            .expressionAttributeValues(Map.of(":pending", s("SECONDARY_PENDING"),
                                    ":decision", s(command.primaryDecision().decisionId()),
                                    ":payload", s(mapper.writeValueAsString(command.primaryDecision())))).build()).build(),
                    TransactWriteItem.builder().put(Put.builder().tableName(STEP).item(item)
                            .conditionExpression("attribute_not_exists(pk)").build()).build()));
            return State.RESERVED;
        } catch (TransactionCanceledException concurrent) {
            var stored = read(STEP, key(command));
            if (stored != null && !stored.isEmpty()) return state(command, stored);
            if (!eligible(command, read(ORIGIN, MessageOriginCodec.key(command.submission().clientMsgId())))) return State.INELIGIBLE;
            throw concurrent;
        }
    }

    public boolean begin(SecondarySendCommand command, Instant now) {
        try {
            db.transactWriteItems(builder -> builder.transactItems(
                    TransactWriteItem.builder().conditionCheck(ConditionCheck.builder().tableName(ORIGIN)
                            .key(MessageOriginCodec.key(command.submission().clientMsgId()))
                            .conditionExpression("#status = :pending AND primary_decision_id = :decision "
                                    + "AND attribute_not_exists(completion_event_id)")
                            .expressionAttributeNames(Map.of("#status", "status"))
                            .expressionAttributeValues(Map.of(":pending", s("SECONDARY_PENDING"),
                                    ":decision", s(command.primaryDecision().decisionId()))).build()).build(),
                    TransactWriteItem.builder().update(Update.builder().tableName(STEP).key(key(command))
                            .conditionExpression("#status = :reserved AND command = :command")
                            .updateExpression("SET #status = :sending, started_at_ms = :started")
                            .expressionAttributeNames(Map.of("#status", "status"))
                            .expressionAttributeValues(Map.of(":reserved", s("RESERVED"), ":sending", s("SENDING"),
                                    ":command", s(mapper.writeValueAsString(command)),
                                    ":started", AttributeValue.fromN(Long.toString(now.toEpochMilli())))).build()).build()));
            return true;
        } catch (TransactionCanceledException concurrent) {
            return false;
        }
    }

    public TcpSendResult record(SecondarySendCommand command, TcpSendResult result) {
        verify(command, result);
        String encoded = mapper.writeValueAsString(result);
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(STEP).key(key(command))
                    .conditionExpression("#status = :sending AND command = :command")
                    .updateExpression("SET #status = :observed, tcp_observation = :result, publish_state = :pending")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(":sending", s("SENDING"), ":observed", s("OBSERVED"),
                            ":command", s(mapper.writeValueAsString(command)), ":result", s(encoded),
                            ":pending", s("PENDING"))).build());
            return result;
        } catch (ConditionalCheckFailedException concurrent) {
            var stored = observation(command);
            if (stored.isPresent()) return stored.get();
            throw concurrent;
        }
    }

    public Optional<TcpSendResult> recoverStale(SecondarySendCommand command, Instant cutoff,
                                                 TcpSendResult timeout) {
        var item = read(STEP, key(command));
        if (item == null || item.isEmpty()) return Optional.empty();
        State current = state(command, item);
        if (current == State.OBSERVED) return observation(command);
        if (current != State.SENDING || !item.containsKey("started_at_ms")
                || Long.parseLong(item.get("started_at_ms").n()) > cutoff.toEpochMilli()) return Optional.empty();
        verify(command, timeout);
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(STEP).key(key(command))
                    .conditionExpression("#status = :sending AND started_at_ms <= :cutoff AND command = :command")
                    .updateExpression("SET #status = :observed, tcp_observation = :result, publish_state = :pending")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(":sending", s("SENDING"), ":observed", s("OBSERVED"),
                            ":cutoff", AttributeValue.fromN(Long.toString(cutoff.toEpochMilli())),
                            ":command", s(mapper.writeValueAsString(command)),
                            ":result", s(mapper.writeValueAsString(timeout)), ":pending", s("PENDING"))).build());
            return Optional.of(timeout);
        } catch (ConditionalCheckFailedException concurrent) {
            return observation(command);
        }
    }

    public Optional<TcpSendResult> observation(SecondarySendCommand command) {
        var item = read(STEP, key(command));
        if (item == null || item.isEmpty()) return Optional.empty();
        state(command, item);
        if (!item.containsKey("tcp_observation")) return Optional.empty();
        var result = mapper.readValue(item.get("tcp_observation").s(), TcpSendResult.class);
        verify(command, result);
        return Optional.of(result);
    }

    public boolean publicationPending(SecondarySendCommand command) {
        var item = read(STEP, key(command));
        state(command, item);
        return "OBSERVED".equals(item.get("status").s())
                && "PENDING".equals(item.getOrDefault("publish_state", s("")).s());
    }

    public boolean published(SecondarySendCommand command) {
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(STEP).key(key(command))
                    .conditionExpression("#status = :observed AND publish_state = :pending AND command = :command")
                    .updateExpression("SET publish_state = :published")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(":observed", s("OBSERVED"), ":pending", s("PENDING"),
                            ":published", s("PUBLISHED"), ":command", s(mapper.writeValueAsString(command)))).build());
            return true;
        } catch (ConditionalCheckFailedException replay) {
            var item = read(STEP, key(command));
            state(command, item);
            if ("PUBLISHED".equals(item.getOrDefault("publish_state", s("")).s())) return false;
            throw replay;
        }
    }

    private boolean eligible(SecondarySendCommand command, Map<String, AttributeValue> origin) {
        return origin != null && !origin.isEmpty() && "SECONDARY_PENDING".equals(origin.getOrDefault("status", s("")).s())
                && command.primaryDecision().decisionId().equals(origin.getOrDefault("primary_decision_id", s("")).s())
                && mapper.writeValueAsString(command.primaryDecision()).equals(origin.getOrDefault("primary_decision", s("")).s())
                && !origin.containsKey("completion_event_id")
                && command.submission().equals(MessageOriginCodec.decode(origin, mapper));
    }

    private State state(SecondarySendCommand command, Map<String, AttributeValue> item) {
        if (item == null || !mapper.writeValueAsString(command).equals(item.getOrDefault("command", s("")).s())) {
            throw new IllegalStateException("TCP STEP command conflict");
        }
        return State.valueOf(item.get("status").s());
    }

    private static void verify(SecondarySendCommand command, TcpSendResult result) {
        if (!TcpSendResult.id(command.submission().clientMsgId(), command.attemptId()).equals(result.resultId())
                || !command.submission().clientMsgId().equals(result.clientMsgId())
                || !command.attemptId().equals(result.attemptId())) {
            throw new IllegalArgumentException("TCP observation does not match command");
        }
    }

    private Map<String, AttributeValue> read(String table, Map<String, AttributeValue> key) {
        return db.getItem(GetItemRequest.builder().tableName(table).key(key).consistentRead(true).build()).item();
    }

    private static Map<String, AttributeValue> key(SecondarySendCommand command) {
        return Map.of("pk", s("DELIVERY#" + command.submission().clientMsgId()),
                "sk", s("TCP_SEND#" + command.attemptId()));
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

package messaging.http.sender;

import messaging.common.delivery.DeliveryIds;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.HttpOutcome;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;
import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

@Repository
public class HttpAttemptRepository {
    public enum State { CLAIMED, IN_PROGRESS, OBSERVED, INELIGIBLE }
    public record Claim(State state, String attemptId, HttpOutcome outcome) { }

    private final DynamoDbClient db;
    private final JsonMapper mapper;

    public HttpAttemptRepository(DynamoDbClient db, JsonMapper mapper) {
        this.db = db;
        this.mapper = mapper;
    }

    public Claim claim(MessageSubmission event, String provider, Instant now, Instant leaseUntil, Instant deadline) {
        String attemptId = DeliveryIds.attemptId(event.executionId(), provider, 1, 1);
        var key = key(event.executionId(), attemptId);
        var origin = db.getItem(GetItemRequest.builder().tableName(ORIGIN)
                .key(MessageOriginCodec.key(event.executionId())).consistentRead(true).build()).item();
        if (origin.isEmpty() || !origin.containsKey(MessageOriginCodec.MESSAGE_ID))
            return new Claim(State.INELIGIBLE, attemptId, null);
        if (!MessageOriginCodec.decode(origin, mapper).equals(event))
            throw new IllegalStateException("Kafka request does not match immutable ORIGIN");
        var item = new HashMap<>(key);
        item.put("schema_version", n(3));
        item.put("delivery_id", s(event.executionId()));
        item.put("message_id", s(event.messageId()));
        item.put("tenant_id", n(event.clientId()));
        item.put("attempt_id", s(attemptId));
        item.put("provider", s(provider));
        item.put("route_order", n(1));
        item.put("retry_count", n(0));
        item.put("version", n(1));
        item.put("status", s("PROCESSING"));
        item.put("deadline_at", n(deadline.toEpochMilli()));
        item.put("lease_until", n(leaseUntil.toEpochMilli()));
        item.put("created_at", s(now.toString()));
        item.put("updated_at", s(now.toString()));
        try {
            db.transactWriteItems(builder -> builder.transactItems(
                    TransactWriteItem.builder().conditionCheck(ConditionCheck.builder().tableName(ORIGIN)
                            .key(MessageOriginCodec.key(event.executionId()))
                            .conditionExpression("delivery_id = :execution AND #status = :received "
                                    + "AND attribute_not_exists(completion_event_id)")
                            .expressionAttributeNames(Map.of("#status", "status"))
                            .expressionAttributeValues(Map.of(":execution", s(event.executionId()),
                                    ":received", s(MessageOriginCodec.STATUS_RECEIVED))).build()).build(),
                    TransactWriteItem.builder().put(Put.builder().tableName(STEP).item(item)
                            .conditionExpression("attribute_not_exists(pk)").build()).build()));
            return new Claim(State.CLAIMED, attemptId, null);
        } catch (TransactionCanceledException collision) {
            if (collision.cancellationReasons().stream().noneMatch(reason ->
                    "ConditionalCheckFailed".equals(reason.code()))) throw collision;
            var existing = db.getItem(GetItemRequest.builder().tableName(STEP).key(key)
                    .consistentRead(true).build()).item();
            if (existing.isEmpty()) return new Claim(State.INELIGIBLE, attemptId, null);
            if (!event.executionId().equals(existing.get("delivery_id").s())
                    || !attemptId.equals(existing.get("attempt_id").s())) {
                throw new IllegalStateException("HTTP STEP key belongs to another execution");
            }
            if (existing.containsKey("outcome_event")) {
                return new Claim(State.OBSERVED, attemptId,
                        mapper.readValue(existing.get("outcome_event").s(), HttpOutcome.class));
            }
            return new Claim(State.IN_PROGRESS, attemptId, null);
        }
    }

    public HttpOutcome record(HttpOutcome outcome) {
        String encoded = mapper.writeValueAsString(outcome);
        var key = key(outcome.executionId(), outcome.attemptId());
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(STEP).key(key)
                    .conditionExpression("#status = :processing AND #version = :version AND attribute_not_exists(outcome_event)")
                    .updateExpression("SET #status = :observed, outcome_event = :event, publish_state = :pending, "
                            + "updated_at = :now REMOVE lease_until")
                    .expressionAttributeNames(Map.of("#status", "status", "#version", "version"))
                    .expressionAttributeValues(Map.of(":processing", s("PROCESSING"), ":observed", s("OBSERVED"),
                            ":version", n(outcome.version()), ":event", s(encoded), ":pending", s("PENDING"),
                            ":now", s(outcome.observedAt().toString()))).build());
            return outcome;
        } catch (ConditionalCheckFailedException changed) {
            var existing = db.getItem(GetItemRequest.builder().tableName(STEP).key(key)
                    .consistentRead(true).build()).item();
            if (existing.containsKey("outcome_event") && encoded.equals(existing.get("outcome_event").s())) return outcome;
            throw changed;
        }
    }

    public void published(HttpOutcome outcome) {
        String encoded = mapper.writeValueAsString(outcome);
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(STEP)
                    .key(key(outcome.executionId(), outcome.attemptId()))
                    .conditionExpression("outcome_event = :event AND publish_state = :pending")
                    .updateExpression("SET publish_state = :published")
                    .expressionAttributeValues(Map.of(":event", s(encoded), ":pending", s("PENDING"),
                            ":published", s("PUBLISHED"))).build());
        } catch (ConditionalCheckFailedException changed) {
            var existing = db.getItem(GetItemRequest.builder().tableName(STEP)
                    .key(key(outcome.executionId(), outcome.attemptId())).consistentRead(true).build()).item();
            if (!encoded.equals(existing.getOrDefault("outcome_event", s("")).s())
                    || !"PUBLISHED".equals(existing.getOrDefault("publish_state", s("")).s())) throw changed;
        }
    }

    private static Map<String, AttributeValue> key(String executionId, String attemptId) {
        return Map.of("pk", s("DELIVERY#" + executionId), "sk", s("ATTEMPT#" + attemptId));
    }
    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
    private static AttributeValue n(long value) { return AttributeValue.fromN(Long.toString(value)); }
}

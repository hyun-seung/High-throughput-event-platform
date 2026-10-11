package messaging.result;

import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.PrimaryStageDecision;
import messaging.common.messages.SecondarySendCommand;
import messaging.common.messages.SecondaryStageDecision;
import messaging.common.messages.TcpSendResult;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;
import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

/** Freezes the TCP response, then directly publishes the final result and customer command. */
@Component
public class SecondaryResultService {
    public enum Outcome { STORED, ALREADY_CLOSED, WAITING }

    private final DynamoDbClient db;
    private final JsonMapper mapper;
    private final ResultKafkaPublisher publisher;

    public SecondaryResultService(DynamoDbClient db, JsonMapper mapper, ResultKafkaPublisher publisher) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
        this.publisher = Objects.requireNonNull(publisher);
    }

    public Outcome process(MessageResultInboxStore.Item item) {
        if (!"TCP_RESPONSE".equals(item.source())) throw new IllegalArgumentException("Expected TCP result");
        var result = mapper.readValue(item.payload(), TcpSendResult.class);
        if (!item.clientMsgId().equals(result.clientMsgId()) || !item.resultId().equals(result.resultId())) {
            throw new IllegalArgumentException("TCP inbox identity mismatch");
        }
        var origin = read(ORIGIN, MessageOriginCodec.key(result.clientMsgId()));
        if (origin == null || origin.isEmpty() || !"SECONDARY_PENDING".equals(origin.getOrDefault("status", s("")).s())) {
            publisher.publish(origin);
            return Outcome.ALREADY_CLOSED;
        }
        var primary = mapper.readValue(origin.get("primary_decision").s(), PrimaryStageDecision.class);
        var submission = MessageOriginCodec.decode(origin, mapper);
        var command = SecondarySendCommand.from(primary, submission);
        if (!command.attemptId().equals(result.attemptId())) {
            throw new IllegalStateException("TCP result does not match frozen secondary command");
        }
        var attempt = read(STEP, Map.of("pk", s("DELIVERY#" + result.clientMsgId()),
                "sk", s("TCP_SEND#" + result.attemptId())));
        if (attempt == null || attempt.isEmpty() || !attempt.containsKey("tcp_observation")) return Outcome.WAITING;
        if (!attempt.containsKey("command")
                || !command.equals(mapper.readValue(attempt.get("command").s(), SecondarySendCommand.class))
                || !result.equals(mapper.readValue(attempt.get("tcp_observation").s(), TcpSendResult.class))) {
            throw new IllegalStateException("TCP result differs from sender observation");
        }
        var secondary = SecondaryStageDecision.from(result);
        String finalStatus = secondary.kind() == SecondaryStageDecision.Kind.SUCCESS
                ? "SECONDARY_SUCCEEDED" : "SECONDARY_FAILED";
        String encoded = mapper.writeValueAsString(secondary);
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(ORIGIN)
                    .key(MessageOriginCodec.key(result.clientMsgId()))
                    .conditionExpression("#status = :pending AND primary_decision_id = :primary "
                            + "AND attribute_not_exists(secondary_decision_id) "
                            + "AND attribute_not_exists(completion_event_id)")
                    .updateExpression("SET #status = :final, secondary_decision_id = :id, "
                            + "secondary_decision = :decision, updated_at = :now")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(":pending", s("SECONDARY_PENDING"),
                            ":primary", s(primary.decisionId()), ":final", s(finalStatus),
                            ":id", s(result.resultId()), ":decision", s(encoded),
                            ":now", s(result.observedAt().toString()))).build());
        } catch (RuntimeException changed) {
            var latest = read(ORIGIN, MessageOriginCodec.key(result.clientMsgId()));
            if (latest != null && latest.containsKey("secondary_decision")) {
                publisher.publish(latest);
                return result.resultId().equals(latest.get("secondary_decision_id").s())
                        ? Outcome.STORED : Outcome.ALREADY_CLOSED;
            }
            if (changed instanceof ConditionalCheckFailedException
                    && (latest == null || !"SECONDARY_PENDING".equals(latest.getOrDefault("status", s("")).s()))) {
                return Outcome.ALREADY_CLOSED;
            }
            throw changed;
        }
        var frozen = new HashMap<>(origin);
        frozen.put("status", s(finalStatus));
        frozen.put("secondary_decision_id", s(result.resultId()));
        frozen.put("secondary_decision", s(encoded));
        publisher.publish(frozen);
        return Outcome.STORED;
    }

    private Map<String, AttributeValue> read(String table, Map<String, AttributeValue> key) {
        return db.getItem(GetItemRequest.builder().tableName(table).key(key).consistentRead(true).build()).item();
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

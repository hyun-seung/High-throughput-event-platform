package messaging.result;

import messaging.common.messages.FinalizedMessageResult;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageResultInboxIndex;
import messaging.common.messages.PrimaryStageDecision;
import messaging.common.messages.SecondarySendCommand;
import messaging.common.messages.SecondaryStageDecision;
import messaging.common.messages.TcpSendResult;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;
import static messaging.common.dynamodb.DynamoDbTableNames.STEP;

/** Freezes the immediate TCP response and a recoverable final-result/customer handoff. */
@Component
public class SecondaryResultService {
    public enum Outcome { STORED, ALREADY_CLOSED, WAITING }

    private final DynamoDbClient db;
    private final JsonMapper mapper;

    public SecondaryResultService(DynamoDbClient db, JsonMapper mapper) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
    }

    public Outcome process(MessageResultInboxStore.Item item) {
        if (!"TCP_RESPONSE".equals(item.source())) throw new IllegalArgumentException("Expected TCP result");
        var result = mapper.readValue(item.payload(), TcpSendResult.class);
        if (!item.clientMsgId().equals(result.clientMsgId()) || !item.resultId().equals(result.resultId())) {
            throw new IllegalArgumentException("TCP inbox identity mismatch");
        }
        var origin = read(ORIGIN, MessageOriginCodec.key(result.clientMsgId()));
        if (origin == null || origin.isEmpty() || !"SECONDARY_PENDING".equals(origin.getOrDefault("status", s("")).s())) {
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
        var finalized = new FinalizedMessageResult(primary, secondary, submission);
        String payload = mapper.writeValueAsString(finalized);
        var outbox = new HashMap<String, AttributeValue>();
        outbox.put("pk", s("DELIVERY#" + result.clientMsgId()));
        outbox.put("sk", s("SECONDARY_DECISION#" + result.resultId()));
        outbox.put("schema_version", AttributeValue.fromN("4"));
        outbox.put("delivery_id", s(result.clientMsgId()));
        outbox.put("result_id", s(result.resultId()));
        outbox.put("source", s("SECONDARY_DECISION"));
        outbox.put("result_payload", s(payload));
        outbox.put("submission_payload", s(mapper.writeValueAsString(submission)));
        outbox.put("status", s("PENDING"));
        outbox.put("received_at", s(result.observedAt().toString()));
        MessageResultInboxIndex.add(outbox, result.clientMsgId(), result.observedAt().toEpochMilli());
        String finalStatus = secondary.kind() == SecondaryStageDecision.Kind.SUCCESS
                ? "SECONDARY_SUCCEEDED" : "SECONDARY_FAILED";
        try {
            db.transactWriteItems(builder -> builder.transactItems(
                    TransactWriteItem.builder().update(Update.builder().tableName(ORIGIN)
                            .key(MessageOriginCodec.key(result.clientMsgId()))
                            .conditionExpression("#status = :pending AND primary_decision_id = :primary "
                                    + "AND attribute_not_exists(secondary_decision_id) "
                                    + "AND attribute_not_exists(completion_event_id)")
                            .updateExpression("SET #status = :final, secondary_decision_id = :id, "
                                    + "secondary_decision = :decision, updated_at = :now")
                            .expressionAttributeNames(Map.of("#status", "status"))
                            .expressionAttributeValues(Map.of(":pending", s("SECONDARY_PENDING"),
                                    ":primary", s(primary.decisionId()), ":final", s(finalStatus),
                                    ":id", s(result.resultId()), ":decision", s(mapper.writeValueAsString(secondary)),
                                    ":now", s(result.observedAt().toString()))).build()).build(),
                    TransactWriteItem.builder().put(Put.builder().tableName(STEP).item(outbox)
                            .conditionExpression("attribute_not_exists(pk)").build()).build()));
            return Outcome.STORED;
        } catch (TransactionCanceledException changed) {
            var latest = read(ORIGIN, MessageOriginCodec.key(result.clientMsgId()));
            var stored = read(STEP, Map.of("pk", outbox.get("pk"), "sk", outbox.get("sk")));
            if (latest != null && stored != null && result.resultId().equals(
                    latest.getOrDefault("secondary_decision_id", s("")).s())
                    && payload.equals(stored.getOrDefault("result_payload", s("")).s())) return Outcome.STORED;
            if (latest == null || !"SECONDARY_PENDING".equals(latest.getOrDefault("status", s("")).s())) {
                return Outcome.ALREADY_CLOSED;
            }
            throw changed;
        }
    }

    private Map<String, AttributeValue> read(String table, Map<String, AttributeValue> key) {
        return db.getItem(GetItemRequest.builder().tableName(table).key(key).consistentRead(true).build()).item();
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

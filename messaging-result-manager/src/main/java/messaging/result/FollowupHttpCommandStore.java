package messaging.result;

import messaging.common.messages.FollowupHttpCommand;
import messaging.common.messages.FollowupDispatchIndex;
import messaging.common.messages.MessageOriginCodec;
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

/** Atomically advances the current result decision and freezes its next HTTP command. */
public class FollowupHttpCommandStore {
    private final DynamoDbClient db;
    private final JsonMapper mapper;

    public FollowupHttpCommandStore(DynamoDbClient db, JsonMapper mapper) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
    }

    /** previousDecisionId is null for the first result; replay returns the already frozen command. */
    public FollowupHttpCommand freeze(String clientMsgId, String previousDecisionId,
                                      FollowupHttpCommand followup) {
        Objects.requireNonNull(clientMsgId);
        Objects.requireNonNull(followup);
        if (!clientMsgId.equals(followup.command().request().clientMsgId())
                || followup.decisionId().equals(previousDecisionId)) {
            throw new IllegalArgumentException("Follow-up command does not advance this message");
        }
        String encoded = mapper.writeValueAsString(followup);
        var item = new HashMap<>(FollowupHttpCommand.key(followup.command()));
        item.put("schema_version", AttributeValue.fromN("4"));
        item.put("delivery_id", s(clientMsgId));
        item.put("decision_id", s(followup.decisionId()));
        item.put("authorization", s(encoded));
        item.put("not_before_ms", AttributeValue.fromN(Long.toString(followup.notBefore().toEpochMilli())));
        FollowupDispatchIndex.add(item, clientMsgId, followup.notBefore().toEpochMilli());
        try {
            var update = Update.builder().tableName(ORIGIN).key(MessageOriginCodec.key(clientMsgId))
                    .conditionExpression("delivery_id = :delivery AND #status = :received "
                            + "AND attribute_exists(pre_send_dispatch) "
                            + "AND attribute_not_exists(completion_event_id) AND "
                            + (previousDecisionId == null
                            ? "attribute_not_exists(#decision)" : "#decision = :previous"))
                    .updateExpression("SET #decision = :next")
                    .expressionAttributeNames(Map.of("#status", "status", "#decision", FollowupHttpCommand.CURRENT_DECISION));
            var values = new HashMap<String, AttributeValue>();
            values.put(":delivery", s(clientMsgId));
            values.put(":received", s(MessageOriginCodec.STATUS_RECEIVED));
            values.put(":next", s(followup.decisionId()));
            if (previousDecisionId != null) values.put(":previous", s(previousDecisionId));
            update.expressionAttributeValues(values);
            db.transactWriteItems(builder -> builder.transactItems(
                    TransactWriteItem.builder().update(update.build()).build(),
                    TransactWriteItem.builder().put(Put.builder().tableName(STEP).item(item)
                            .conditionExpression("attribute_not_exists(pk)").build()).build()));
            return followup;
        } catch (TransactionCanceledException changed) {
            if (!changed.cancellationReasons().stream()
                    .anyMatch(reason -> "ConditionalCheckFailed".equals(reason.code()))) throw changed;
            var origin = read(ORIGIN, MessageOriginCodec.key(clientMsgId));
            var stored = read(STEP, FollowupHttpCommand.key(followup.command()));
            if (followup.decisionId().equals(origin.getOrDefault(FollowupHttpCommand.CURRENT_DECISION, s("")).s())
                    && encoded.equals(stored.getOrDefault("authorization", s("")).s())) return followup;
            throw new IllegalStateException("Follow-up HTTP decision conflicts with stored state", changed);
        }
    }

    private Map<String, AttributeValue> read(String table, Map<String, AttributeValue> key) {
        return db.getItem(GetItemRequest.builder().tableName(table).key(key).consistentRead(true).build()).item();
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

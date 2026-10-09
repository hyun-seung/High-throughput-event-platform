package messaging.pre.send;

import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.PreSendFailure;
import messaging.common.messages.PreSendDispatch;
import messaging.common.messages.HttpCarrier;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;

/** Freezes the command or rejection before Kafka publication. */
public class PreSendDecisionStore {
    static final String DISPATCH = "pre_send_dispatch";
    static final String CARRIER = "initial_http_carrier";
    static final String MAPPED = "initial_http_carrier_mapped";
    private final DynamoDbClient db;
    private final JsonMapper mapper;
    private final PreSendPreparation preparation;
    private final Clock clock;

    public PreSendDecisionStore(DynamoDbClient db, JsonMapper mapper,
                                PreSendPreparation preparation, Clock clock) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
        this.preparation = Objects.requireNonNull(preparation);
        this.clock = Objects.requireNonNull(clock);
    }

    public Optional<PreSendDispatch> prepareOrLoad(MessageSubmission admission) {
        Map<String, AttributeValue> current = read(admission);
        if (!eligible(current)) return Optional.empty();
        if (current.containsKey(DISPATCH)) return Optional.of(decode(current));

        PreSendPreparation.Decision decision = preparation.prepare(admission, storedCarrier(current));
        PreSendDispatch dispatch;
        if (decision instanceof PreSendPreparation.Ready ready) {
            dispatch = new PreSendDispatch(ready.command(), null);
        } else {
            PreSendFailure.Reason reason = decision instanceof PreSendPreparation.Expired
                    ? PreSendFailure.Reason.PRIMARY_EXPIRED
                    : PreSendFailure.Reason.valueOf(((PreSendPreparation.Rejected) decision).reason().name());
            dispatch = new PreSendDispatch(null,
                    PreSendFailure.of(admission.clientMsgId(), reason, clock.instant()));
        }
        var names = new HashMap<>(Map.of("#status", "status", "#dispatch", DISPATCH));
        var values = new HashMap<>(Map.of(
                ":execution", AttributeValue.fromS(admission.clientMsgId()),
                ":received", AttributeValue.fromS(MessageOriginCodec.STATUS_RECEIVED),
                ":dispatch", AttributeValue.fromS(mapper.writeValueAsString(dispatch))));
        String condition = "delivery_id = :execution AND #status = :received "
                + "AND attribute_not_exists(completion_event_id) AND attribute_not_exists(#dispatch)";
        String update = "SET #dispatch = :dispatch";
        if (decision instanceof PreSendPreparation.Ready ready) {
            names.put("#carrier", CARRIER);
            names.put("#mapped", MAPPED);
            values.put(":carrier", AttributeValue.fromS(ready.command().carrier().name()));
            values.put(":mapped", AttributeValue.fromBool(ready.carrierMapped()));
            // Preserve an older deployment's frozen route, including a concurrent legacy write.
            condition += " AND ((attribute_not_exists(#carrier) AND attribute_not_exists(#mapped)) "
                    + "OR (#carrier = :carrier AND #mapped = :mapped))";
            update += ", #carrier = :carrier, #mapped = :mapped";
        }
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(ORIGIN)
                    .key(MessageOriginCodec.key(admission.clientMsgId()))
                    .conditionExpression(condition).updateExpression(update)
                    .expressionAttributeNames(names).expressionAttributeValues(values).build());
            return Optional.of(dispatch);
        } catch (ConditionalCheckFailedException changed) {
            current = read(admission);
            if (!eligible(current)) return Optional.empty();
            if (current.containsKey(DISPATCH)) return Optional.of(decode(current));
            throw new IllegalStateException("Pre-send decision condition failed without a stored outcome", changed);
        }
    }

    private static CarrierResolution storedCarrier(Map<String, AttributeValue> item) {
        if (!item.containsKey(CARRIER) && !item.containsKey(MAPPED)) return null;
        if (!item.containsKey(CARRIER) || !item.containsKey(MAPPED) || item.get(MAPPED).bool() == null) {
            throw new IllegalStateException("Incomplete initial carrier selection");
        }
        return new CarrierResolution(HttpCarrier.valueOf(item.get(CARRIER).s()), item.get(MAPPED).bool());
    }

    private Map<String, AttributeValue> read(MessageSubmission admission) {
        Map<String, AttributeValue> item = db.getItem(GetItemRequest.builder().tableName(ORIGIN)
                .key(MessageOriginCodec.key(admission.clientMsgId())).consistentRead(true).build()).item();
        if (item != null && item.containsKey(MessageOriginCodec.MESSAGE_ID)
                && !MessageOriginCodec.decode(item, mapper).equals(admission)) {
            throw new IllegalStateException("Kafka admission does not match immutable ORIGIN");
        }
        return item;
    }

    private static boolean eligible(Map<String, AttributeValue> item) {
        return item != null && item.containsKey(MessageOriginCodec.MESSAGE_ID)
                && !item.containsKey("completion_event_id")
                && MessageOriginCodec.STATUS_RECEIVED.equals(
                        item.getOrDefault("status", AttributeValue.fromS("")).s());
    }

    private PreSendDispatch decode(Map<String, AttributeValue> item) {
        return mapper.readValue(item.get(DISPATCH).s(), PreSendDispatch.class);
    }
}

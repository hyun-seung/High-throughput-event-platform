package messaging.pre.send.reference;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageSubmission;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;

/** Freezes the first HTTP carrier on ORIGIN before a send command can be prepared. */
public class InitialCarrierStore {
    public static final String CARRIER = "initial_http_carrier";
    public static final String MAPPED = "initial_http_carrier_mapped";

    private final DynamoDbClient db;
    private final JsonMapper mapper;

    public InitialCarrierStore(DynamoDbClient db, JsonMapper mapper) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
    }

    /** Empty means this execution no longer has an eligible ORIGIN. */
    public Optional<CarrierResolution> resolve(MessageSubmission admission, Supplier<CarrierResolution> choose) {
        Objects.requireNonNull(admission);
        Objects.requireNonNull(choose);
        Map<String, AttributeValue> current = read(admission.clientMsgId());
        if (!eligible(current)) return Optional.empty();
        verifyAdmission(current, admission);
        if (current.containsKey(CARRIER)) return Optional.of(decode(current));

        CarrierResolution selected = Objects.requireNonNull(choose.get());
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(ORIGIN)
                    .key(MessageOriginCodec.key(admission.clientMsgId()))
                    .conditionExpression("delivery_id = :execution AND #status = :received "
                            + "AND attribute_not_exists(completion_event_id) AND attribute_not_exists(#carrier)")
                    .updateExpression("SET #carrier = :carrier, #mapped = :mapped")
                    .expressionAttributeNames(Map.of("#status", "status", "#carrier", CARRIER,
                            "#mapped", MAPPED))
                    .expressionAttributeValues(Map.of(
                            ":execution", AttributeValue.fromS(admission.clientMsgId()),
                            ":received", AttributeValue.fromS(MessageOriginCodec.STATUS_RECEIVED),
                            ":carrier", AttributeValue.fromS(selected.carrier().name()),
                            ":mapped", AttributeValue.fromBool(selected.mapped()))).build());
            return Optional.of(selected);
        } catch (ConditionalCheckFailedException changed) {
            // Another consumer may have selected a carrier or the execution may have closed.
            current = read(admission.clientMsgId());
            if (!eligible(current)) return Optional.empty();
            verifyAdmission(current, admission);
            if (current.containsKey(CARRIER)) return Optional.of(decode(current));
            throw new IllegalStateException("Initial carrier condition failed without a stored selection", changed);
        }
    }

    private Map<String, AttributeValue> read(String clientMsgId) {
        return db.getItem(GetItemRequest.builder().tableName(ORIGIN)
                .key(MessageOriginCodec.key(clientMsgId)).consistentRead(true).build()).item();
    }

    private static boolean eligible(Map<String, AttributeValue> item) {
        return item != null && item.containsKey(MessageOriginCodec.MESSAGE_ID)
                && !item.containsKey("completion_event_id")
                && MessageOriginCodec.STATUS_RECEIVED.equals(
                        item.getOrDefault("status", AttributeValue.fromS("")).s());
    }

    private void verifyAdmission(Map<String, AttributeValue> current, MessageSubmission admission) {
        if (!MessageOriginCodec.decode(current, mapper).equals(admission)) {
            throw new IllegalStateException("Kafka admission does not match immutable ORIGIN");
        }
    }

    private static CarrierResolution decode(Map<String, AttributeValue> item) {
        if (!item.containsKey(MAPPED)) throw new IllegalStateException("Incomplete initial carrier selection");
        try {
            return new CarrierResolution(HttpCarrier.valueOf(item.get(CARRIER).s()), item.get(MAPPED).bool());
        } catch (RuntimeException malformed) {
            throw new IllegalStateException("Invalid initial carrier selection", malformed);
        }
    }
}

package event.delivery.ingress.repository;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryPayloads;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static event.common.dynamodb.DynamoDbAttributeNames.CREATED_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.DELIVERY_ID;
import static event.common.dynamodb.DynamoDbAttributeNames.DELIVERY_TYPE;
import static event.common.dynamodb.DynamoDbAttributeNames.EVENT_ID;
import static event.common.dynamodb.DynamoDbAttributeNames.EVENT_TYPE;
import static event.common.dynamodb.DynamoDbAttributeNames.OCCURRED_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.PAYLOAD;
import static event.common.dynamodb.DynamoDbAttributeNames.FALLBACK_ALLOWED;
import static event.common.dynamodb.DynamoDbAttributeNames.PK;
import static event.common.dynamodb.DynamoDbAttributeNames.SK;
import static event.common.dynamodb.DynamoDbAttributeNames.STATUS;
import static event.common.dynamodb.DynamoDbAttributeNames.TENANT_ID;
import static event.common.dynamodb.DynamoDbAttributeNames.UPDATED_AT;
import static event.common.dynamodb.DynamoDbTableNames.DELIVERY_STATE;

@Repository
@RequiredArgsConstructor
public class DeliveryRepository {

    private static final String DELIVERY_PREFIX = "DELIVERY#";
    private static final String META = "META";
    private static final String ACCEPTED = "ACCEPTED";

    private final DynamoDbClient dynamoDbClient;
    private final JsonMapper jsonMapper;

    public DeliveryEvent saveOrLoad(DeliveryEvent event) {
        Map<String, AttributeValue> item = toItem(event);
        PutItemRequest request = PutItemRequest.builder()
                .tableName(DELIVERY_STATE)
                .item(item)
                .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                .expressionAttributeNames(Map.of("#pk", PK, "#sk", SK))
                .build();

        try {
            dynamoDbClient.putItem(request);
            return event;
        } catch (ConditionalCheckFailedException e) {
            return loadExistingDelivery(event);
        }
    }

    private DeliveryEvent loadExistingDelivery(DeliveryEvent event) {
        Map<String, AttributeValue> existing = dynamoDbClient.getItem(GetItemRequest.builder()
                        .tableName(DELIVERY_STATE)
                        .key(key(event.deliveryId()))
                        .consistentRead(true)
                        .build())
                .item();

        if (existing.isEmpty()) {
            throw new IllegalStateException(
                    "Existing delivery disappeared during duplicate verification. deliveryId=" + event.deliveryId());
        }

        boolean sameRequest = value(existing, EVENT_ID).equals(event.eventId())
                && value(existing, TENANT_ID).equals(String.valueOf(event.tenantId()))
                && value(existing, DELIVERY_TYPE).equals(event.deliveryType())
                && Boolean.TRUE.equals(existing.getOrDefault(FALLBACK_ALLOWED, AttributeValue.fromBool(false)).bool()) == event.fallbackAllowed()
                && value(existing, PAYLOAD).equals(serializePayload(event.payload()));

        if (!sameRequest) {
            throw new IdempotencyConflictException(event.deliveryId());
        }

        // A client retry has a new API timestamp. Always forward the first persisted ingress time.
        Instant originalOccurredAt = Instant.parse(value(existing, OCCURRED_AT));
        return new DeliveryEvent(
                event.schemaVersion(), event.eventId(), event.eventType(), event.deliveryId(),
                event.tenantId(), event.deliveryType(), event.payload(), originalOccurredAt,
                event.correlationId(), event.causationId(), event.fallbackAllowed());
    }

    private Map<String, AttributeValue> toItem(DeliveryEvent event) {
        Instant now = Instant.now();
        Map<String, AttributeValue> item = new HashMap<>();

        item.putAll(key(event.deliveryId()));
        item.put(DELIVERY_ID, AttributeValue.fromS(event.deliveryId()));
        item.put(EVENT_ID, AttributeValue.fromS(event.eventId()));
        item.put(EVENT_TYPE, AttributeValue.fromS(event.eventType().name()));
        item.put(TENANT_ID, AttributeValue.fromN(String.valueOf(event.tenantId())));
        item.put(DELIVERY_TYPE, AttributeValue.fromS(event.deliveryType()));
        item.put(PAYLOAD, AttributeValue.fromS(serializePayload(event.payload())));
        item.put(FALLBACK_ALLOWED, AttributeValue.fromBool(event.fallbackAllowed()));
        item.put(OCCURRED_AT, AttributeValue.fromS(event.occurredAt().toString()));
        item.put(STATUS, AttributeValue.fromS(ACCEPTED));
        item.put(CREATED_AT, AttributeValue.fromS(now.toString()));
        item.put(UPDATED_AT, AttributeValue.fromS(now.toString()));

        return item;
    }

    private Map<String, AttributeValue> key(String deliveryId) {
        return Map.of(
                PK, AttributeValue.fromS(DELIVERY_PREFIX + deliveryId),
                SK, AttributeValue.fromS(META)
        );
    }

    private String value(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        return value == null ? "" : value.s() == null ? value.n() : value.s();
    }

    private String serializePayload(Map<String, Object> payload) {
        try {
            return jsonMapper.writeValueAsString(DeliveryPayloads.canonicalize(payload));
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to serialize delivery payload.", e);
        }
    }
}

package event.delivery.dispatch.repository;

import event.common.delivery.DeliveryEvent;
import event.delivery.dispatch.model.DispatchAttempt;
import event.delivery.dispatch.model.DispatchClaim;
import event.delivery.dispatch.port.DispatchAttemptStore;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import java.time.Instant;
import java.util.Map;

import static event.common.dynamodb.DynamoDbAttributeNames.ATTEMPT_ID;
import static event.common.dynamodb.DynamoDbAttributeNames.CREATED_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.DELIVERY_ID;
import static event.common.dynamodb.DynamoDbAttributeNames.EVENT_ID;
import static event.common.dynamodb.DynamoDbAttributeNames.LEASE_UNTIL;
import static event.common.dynamodb.DynamoDbAttributeNames.PK;
import static event.common.dynamodb.DynamoDbAttributeNames.PROVIDER;
import static event.common.dynamodb.DynamoDbAttributeNames.PROVIDER_PROCESSED_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.SK;
import static event.common.dynamodb.DynamoDbAttributeNames.STATUS;
import static event.common.dynamodb.DynamoDbAttributeNames.UPDATED_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.VERSION;
import static event.common.dynamodb.DynamoDbTableNames.DELIVERY_STATE;

@Repository
@RequiredArgsConstructor
public class DispatchAttemptRepository implements DispatchAttemptStore {

    private static final String DELIVERY_PREFIX = "DELIVERY#";
    private static final String ATTEMPT_PREFIX = "ATTEMPT#";
    private static final String PROCESSING = "PROCESSING";
    private static final String ACCEPTED = "ACCEPTED";

    private final DynamoDbClient dynamoDbClient;

    @Override
    public DispatchClaim claim(
            DeliveryEvent event,
            String attemptId,
            String provider,
            Instant now,
            Instant leaseUntil
    ) {
        Map<String, AttributeValue> key = key(event.deliveryId(), attemptId);
        Map<String, String> names = Map.ofEntries(
                Map.entry("#pk", PK),
                Map.entry("#attemptId", ATTEMPT_ID),
                Map.entry("#deliveryId", DELIVERY_ID),
                Map.entry("#eventId", EVENT_ID),
                Map.entry("#provider", PROVIDER),
                Map.entry("#status", STATUS),
                Map.entry("#leaseUntil", LEASE_UNTIL),
                Map.entry("#createdAt", CREATED_AT),
                Map.entry("#updatedAt", UPDATED_AT),
                Map.entry("#version", VERSION)
        );
        Map<String, AttributeValue> values = Map.ofEntries(
                Map.entry(":attemptId", text(attemptId)),
                Map.entry(":deliveryId", text(event.deliveryId())),
                Map.entry(":eventId", text(event.eventId())),
                Map.entry(":provider", text(provider)),
                Map.entry(":processing", text(PROCESSING)),
                Map.entry(":leaseUntil", number(leaseUntil.toEpochMilli())),
                Map.entry(":now", text(now.toString())),
                Map.entry(":nowEpochMillis", number(now.toEpochMilli())),
                Map.entry(":zero", number(0)),
                Map.entry(":one", number(1))
        );

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(DELIVERY_STATE)
                .key(key)
                .conditionExpression(
                        "attribute_not_exists(#pk) OR (#status = :processing AND #leaseUntil <= :nowEpochMillis)")
                .updateExpression("SET #attemptId = :attemptId, #deliveryId = :deliveryId, "
                        + "#eventId = :eventId, #provider = :provider, #status = :processing, "
                        + "#leaseUntil = :leaseUntil, #createdAt = if_not_exists(#createdAt, :now), "
                        + "#updatedAt = :now, #version = if_not_exists(#version, :zero) + :one")
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .returnValues(ReturnValue.ALL_NEW)
                .build();

        try {
            UpdateItemResponse response = dynamoDbClient.updateItem(request);
            long version = Long.parseLong(response.attributes().get(VERSION).n());
            return DispatchClaim.claimed(new DispatchAttempt(
                    event.deliveryId(), attemptId, provider, version));
        } catch (ConditionalCheckFailedException e) {
            return existingClaim(key);
        }
    }

    @Override
    public void markAccepted(DispatchAttempt attempt, Instant providerProcessedAt, Instant now) {
        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(DELIVERY_STATE)
                .key(key(attempt.deliveryId(), attempt.attemptId()))
                .conditionExpression("#status = :processing AND #version = :version")
                .updateExpression("SET #status = :accepted, #providerProcessedAt = :providerProcessedAt, "
                        + "#updatedAt = :now REMOVE #leaseUntil")
                .expressionAttributeNames(Map.of(
                        "#status", STATUS,
                        "#version", VERSION,
                        "#providerProcessedAt", PROVIDER_PROCESSED_AT,
                        "#updatedAt", UPDATED_AT,
                        "#leaseUntil", LEASE_UNTIL
                ))
                .expressionAttributeValues(Map.of(
                        ":processing", text(PROCESSING),
                        ":accepted", text(ACCEPTED),
                        ":version", number(attempt.version()),
                        ":providerProcessedAt", text(providerProcessedAt.toString()),
                        ":now", text(now.toString())
                ))
                .build();

        dynamoDbClient.updateItem(request);
    }

    private DispatchClaim existingClaim(Map<String, AttributeValue> key) {
        Map<String, AttributeValue> item = dynamoDbClient.getItem(GetItemRequest.builder()
                        .tableName(DELIVERY_STATE)
                        .key(key)
                        .consistentRead(true)
                        .build())
                .item();

        AttributeValue status = item.get(STATUS);
        if (status != null && ACCEPTED.equals(status.s())) {
            return DispatchClaim.alreadyAccepted();
        }
        return DispatchClaim.inProgress();
    }

    private Map<String, AttributeValue> key(String deliveryId, String attemptId) {
        return Map.of(
                PK, text(DELIVERY_PREFIX + deliveryId),
                SK, text(ATTEMPT_PREFIX + attemptId)
        );
    }

    private AttributeValue text(String value) {
        return AttributeValue.fromS(value);
    }

    private AttributeValue number(long value) {
        return AttributeValue.fromN(Long.toString(value));
    }
}

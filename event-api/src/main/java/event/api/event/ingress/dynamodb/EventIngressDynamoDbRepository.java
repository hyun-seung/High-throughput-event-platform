package event.api.event.ingress.dynamodb;

import event.common.dynamodb.RecoveryBucketResolver;
import event.common.dynamodb.domain.EventSource;
import event.common.dynamodb.domain.EventStepType;
import event.common.dynamodb.domain.PublishStatus;
import event.common.dynamodb.domain.StepStatus;
import event.common.message.EventMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static event.common.dynamodb.DynamoDbAttributeNames.CREATED_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.EVENT_ID;
import static event.common.dynamodb.DynamoDbAttributeNames.EVENT_TYPE;
import static event.common.dynamodb.DynamoDbAttributeNames.OCCURRED_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.PAYLOAD;
import static event.common.dynamodb.DynamoDbAttributeNames.PK;
import static event.common.dynamodb.DynamoDbAttributeNames.PUBLISH_STATUS;
import static event.common.dynamodb.DynamoDbAttributeNames.RECOVERY_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.RECOVERY_BUCKET;
import static event.common.dynamodb.DynamoDbAttributeNames.RETRY_COUNT;
import static event.common.dynamodb.DynamoDbAttributeNames.SK;
import static event.common.dynamodb.DynamoDbAttributeNames.SOURCE;
import static event.common.dynamodb.DynamoDbAttributeNames.STATUS;
import static event.common.dynamodb.DynamoDbAttributeNames.UPDATED_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.USER_ID;
import static event.common.dynamodb.DynamoDbTableNames.EVENT_MESSAGE;
import static event.common.dynamodb.DynamoDbTableNames.EVENT_STEP;

@Slf4j
@Repository
@RequiredArgsConstructor
public class EventIngressDynamoDbRepository {

    private static final String EVENT_KEY_PREFIX = "EVENT#";

    private static final Duration INITIAL_RECOVERY_DELAY = Duration.ofSeconds(5);
    private static final Duration RETRY_DELAY = Duration.ofSeconds(5);

    private final DynamoDbClient dynamoDbClient;
    private final JsonMapper jsonMapper;

    public void save(EventMessage message) {
        Instant now = Instant.now();
        Instant recoveryAt = now.plus(INITIAL_RECOVERY_DELAY);

        TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
                .transactItems(
                        createEventMessageItem(message, now),
                        createIngressStepItem(message.eventId(), now, recoveryAt)
                )
                .build();

        dynamoDbClient.transactWriteItems(request);

        log.debug("Event stored in DynamoDB transaction. eventId={}, userId={}, eventType={}",
                message.eventId(), message.userId(), message.eventType());
    }

    public void markPublished(String eventId) {
        Instant now = Instant.now();

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(EVENT_STEP)
                .key(createIngressKey(eventId))
                .updateExpression(
                        "SET #publishStatus = :published, #updatedAt = :updatedAt " +
                                "REMOVE #recoveryBucket, #recoveryAt"
                )
                .conditionExpression("attribute_exists(#pk) AND attribute_exists(#sk)")
                .expressionAttributeNames(Map.of(
                        "#pk", PK,
                        "#sk", SK,
                        "#publishStatus", PUBLISH_STATUS,
                        "#updatedAt", UPDATED_AT,
                        "#recoveryBucket", RECOVERY_BUCKET,
                        "#recoveryAt", RECOVERY_AT
                ))
                .expressionAttributeValues(Map.of(
                        ":published", AttributeValue.fromS(PublishStatus.PUBLISHED.name()),
                        ":updatedAt", AttributeValue.fromS(now.toString())
                ))
                .build();

        dynamoDbClient.updateItem(request);

        log.debug("Kafka publish marked as published. eventId={}", eventId);
    }

    public void markRetryRequired(String eventId) {
        Instant now = Instant.now();
        Instant recoveryAt = now.plus(RETRY_DELAY);

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(EVENT_STEP)
                .key(createIngressKey(eventId))
                .updateExpression(
                        "SET #publishStatus = :retryRequired, " +
                                "#retryCount = if_not_exists(#retryCount, :zero) + :one, " +
                                "#recoveryAt = :recoveryAt, " +
                                "#updatedAt = :updatedAt"
                )
                .conditionExpression("#publishStatus = :pending")
                .expressionAttributeNames(Map.of(
                        "#publishStatus", PUBLISH_STATUS,
                        "#retryCount", RETRY_COUNT,
                        "#recoveryAt", RECOVERY_AT,
                        "#updatedAt", UPDATED_AT
                ))
                .expressionAttributeValues(Map.of(
                        ":pending", AttributeValue.fromS(PublishStatus.PENDING.name()),
                        ":retryRequired", AttributeValue.fromS(PublishStatus.RETRY_REQUIRED.name()),
                        ":zero", AttributeValue.fromN("0"),
                        ":one", AttributeValue.fromN("1"),
                        ":recoveryAt", AttributeValue.fromS(recoveryAt.toString()),
                        ":updatedAt", AttributeValue.fromS(now.toString())
                ))
                .build();

        dynamoDbClient.updateItem(request);

        log.debug("Kafka publish marked as retry required. eventId={}, recoveryAt={}", eventId, recoveryAt);
    }

    private TransactWriteItem createEventMessageItem(
            EventMessage message,
            Instant now
    ) {
        Map<String, AttributeValue> item = new HashMap<>();

        item.put(EVENT_ID, AttributeValue.fromS(message.eventId()));
        item.put(SOURCE, AttributeValue.fromS(EventSource.DIRECT_API.name()));
        item.put(USER_ID, AttributeValue.fromN(String.valueOf(message.userId())));
        item.put(EVENT_TYPE, AttributeValue.fromS(message.eventType()));
        item.put(PAYLOAD, AttributeValue.fromS(serializePayload(message.payload())));
        item.put(OCCURRED_AT, AttributeValue.fromS(message.occurredAt().toString()));
        item.put(CREATED_AT, AttributeValue.fromS(now.toString()));

        Put put = Put.builder()
                .tableName(EVENT_MESSAGE)
                .item(item)
                .conditionExpression("attribute_not_exists(event_id)")
                .build();

        return TransactWriteItem.builder()
                .put(put)
                .build();
    }

    private TransactWriteItem createIngressStepItem(
            String eventId,
            Instant now,
            Instant recoveryAt
    ) {
        Map<String, AttributeValue> item = new HashMap<>();

        item.put(PK, AttributeValue.fromS(EVENT_KEY_PREFIX + eventId));
        item.put(SK, AttributeValue.fromS(EventStepType.INGRESS.sortKey()));

        item.put(STATUS, AttributeValue.fromS(StepStatus.COMPLETED.name()));
        item.put(PUBLISH_STATUS, AttributeValue.fromS(PublishStatus.PENDING.name()));

        item.put(RECOVERY_BUCKET, AttributeValue.fromS(RecoveryBucketResolver.resolve(eventId)));
        item.put(RECOVERY_AT, AttributeValue.fromS(recoveryAt.toString()));

        item.put(RETRY_COUNT, AttributeValue.fromN("0"));
        item.put(CREATED_AT, AttributeValue.fromS(now.toString()));
        item.put(UPDATED_AT, AttributeValue.fromS(now.toString()));

        Put put = Put.builder()
                .tableName(EVENT_STEP)
                .item(item)
                .conditionExpression("attribute_not_exists(pk) AND attribute_not_exists(sk)")
                .build();

        return TransactWriteItem.builder()
                .put(put)
                .build();
    }

    private Map<String, AttributeValue> createIngressKey(String eventId) {
        return Map.of(
                PK, AttributeValue.fromS(EVENT_KEY_PREFIX + eventId),
                SK, AttributeValue.fromS(EventStepType.INGRESS.sortKey())
        );
    }

    private String serializePayload(Map<String, Object> payload) {
        try {
            return jsonMapper.writeValueAsString(payload);
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to serialize event payload.", e);
        }
    }
}

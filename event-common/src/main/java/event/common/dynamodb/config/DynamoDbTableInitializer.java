package event.common.dynamodb.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;

import static event.common.dynamodb.DynamoDbAttributeNames.EVENT_ID;
import static event.common.dynamodb.DynamoDbAttributeNames.PK;
import static event.common.dynamodb.DynamoDbAttributeNames.RECOVERY_AT;
import static event.common.dynamodb.DynamoDbAttributeNames.RECOVERY_BUCKET;
import static event.common.dynamodb.DynamoDbAttributeNames.SK;
import static event.common.dynamodb.DynamoDbIndexNames.RECOVERY_INDEX;
import static event.common.dynamodb.DynamoDbTableNames.EVENT_INGRESS;
import static event.common.dynamodb.DynamoDbTableNames.EVENT_MESSAGE;
import static event.common.dynamodb.DynamoDbTableNames.EVENT_STEP;

@Slf4j
@RequiredArgsConstructor
public class DynamoDbTableInitializer implements ApplicationRunner {

    private final DynamoDbClient dynamoDbClient;

    @Override
    public void run(ApplicationArguments args) {
        initializeEventIngressTable();
        initializeEventMessageTable();
        initializeEventStepTable();
    }

    private void initializeEventIngressTable() {
        DescribeTableResponse response = describeTable(EVENT_INGRESS);

        if (response == null) {
            createEventIngressTable();
            return;
        }

        enableStreamIfNecessary(response);
    }

    private void initializeEventMessageTable() {
        if (describeTable(EVENT_MESSAGE) != null) {
            log.debug("DynamoDB table already exists. table={}", EVENT_MESSAGE);
            return;
        }

        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(EVENT_MESSAGE)
                .attributeDefinitions(attribute(EVENT_ID, ScalarAttributeType.S))
                .keySchema(partitionKey(EVENT_ID))
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build();

        createTable(request);
    }

    private void initializeEventStepTable() {
        if (describeTable(EVENT_STEP) != null) {
            log.debug("DynamoDB table already exists. table={}", EVENT_STEP);
            return;
        }

        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(EVENT_STEP)
                .attributeDefinitions(
                        attribute(PK, ScalarAttributeType.S),
                        attribute(SK, ScalarAttributeType.S),
                        attribute(RECOVERY_BUCKET, ScalarAttributeType.S),
                        attribute(RECOVERY_AT, ScalarAttributeType.S)
                )
                .keySchema(partitionKey(PK), sortKey(SK))
                .globalSecondaryIndexes(
                        GlobalSecondaryIndex.builder()
                                .indexName(RECOVERY_INDEX)
                                .keySchema(partitionKey(RECOVERY_BUCKET), sortKey(RECOVERY_AT))
                                .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                                .build()
                )
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build();

        createTable(request);
    }

    private void createEventIngressTable() {
        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(EVENT_INGRESS)
                .attributeDefinitions(attribute(EVENT_ID, ScalarAttributeType.S))
                .keySchema(partitionKey(EVENT_ID))
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .streamSpecification(
                        StreamSpecification.builder()
                                .streamEnabled(true)
                                .streamViewType(StreamViewType.NEW_IMAGE)
                                .build()
                )
                .build();

        createTable(request);
    }

    private void createTable(CreateTableRequest request) {
        dynamoDbClient.createTable(request);

        dynamoDbClient.waiter()
                .waitUntilTableExists(builder ->
                        builder.tableName(request.tableName())
                );

        log.info("DynamoDB table created. table={}", request.tableName());
    }

    private DescribeTableResponse describeTable(String tableName) {
        try {
            return dynamoDbClient.describeTable(builder -> builder.tableName(tableName));
        } catch (ResourceNotFoundException e) {
            return null;
        }
    }

    private void enableStreamIfNecessary(DescribeTableResponse response) {
        StreamSpecification specification = response.table().streamSpecification();

        if (specification != null
                && Boolean.TRUE.equals(specification.streamEnabled())
                && specification.streamViewType() == StreamViewType.NEW_IMAGE) {

            log.debug("DynamoDB stream already enabled. table={}", EVENT_INGRESS);
            return;
        }

        UpdateTableRequest request = UpdateTableRequest.builder()
                .tableName(EVENT_INGRESS)
                .streamSpecification(
                        StreamSpecification.builder()
                                .streamEnabled(true)
                                .streamViewType(StreamViewType.NEW_IMAGE)
                                .build()
                )
                .build();

        dynamoDbClient.updateTable(request);

        log.info("DynamoDB stream enabled. table={}, streamViewType={}", EVENT_INGRESS, StreamViewType.NEW_IMAGE);
    }

    private AttributeDefinition attribute(String name, ScalarAttributeType type) {
        return AttributeDefinition.builder()
                .attributeName(name)
                .attributeType(type)
                .build();
    }

    private KeySchemaElement partitionKey(String name) {
        return KeySchemaElement.builder()
                .attributeName(name)
                .keyType(KeyType.HASH)
                .build();
    }

    private KeySchemaElement sortKey(String name) {
        return KeySchemaElement.builder()
                .attributeName(name)
                .keyType(KeyType.RANGE)
                .build();
    }
}
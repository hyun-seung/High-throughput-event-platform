package messaging.common.dynamodb.config;

import messaging.common.messages.MessagePublicationIndex;
import messaging.common.messages.FollowupDispatchIndex;
import messaging.common.messages.MessageResultInboxIndex;
import messaging.common.messages.PrimaryExpiryIndex;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateGlobalSecondaryIndexAction;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexUpdate;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;

import static messaging.common.dynamodb.DynamoDbAttributeNames.PK;
import static messaging.common.dynamodb.DynamoDbAttributeNames.SK;
import static messaging.common.dynamodb.DynamoDbTableNames.*;

@Slf4j
public class DynamoDbTableInitializer implements ApplicationRunner {

    private final DynamoDbClient dynamoDbClient;
    private final boolean enableTimeToLive;

    public DynamoDbTableInitializer(DynamoDbClient dynamoDbClient) {
        this(dynamoDbClient, false);
    }

    public DynamoDbTableInitializer(DynamoDbClient dynamoDbClient, boolean enableTimeToLive) {
        this.dynamoDbClient = dynamoDbClient;
        this.enableTimeToLive = enableTimeToLive;
    }

    @Override
    public void run(ApplicationArguments args) {
        for (String table : java.util.List.of(ORIGIN, STEP)) createIfMissing(table);
    }

    private void createIfMissing(String table) {
        if (tableExists(table)) {
            if (ORIGIN.equals(table)) {
                createPublicationIndexIfMissing();
                createPrimaryExpiryIndexIfMissing();
            }
            if (STEP.equals(table)) {
                createFollowupIndexIfMissing();
                createResultInboxIndexIfMissing();
            }
            if (enableTimeToLive) enableTimeToLive(table);
            log.debug("DynamoDB table already exists. table={}", table);
            return;
        }

        var definitions = new java.util.ArrayList<>(java.util.List.of(
                AttributeDefinition.builder().attributeName(PK).attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName(SK).attributeType(ScalarAttributeType.S).build()));
        var indexes = new java.util.ArrayList<software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex>();
        if (ORIGIN.equals(table)) {
            definitions.add(AttributeDefinition.builder().attributeName(MessagePublicationIndex.BUCKET).attributeType(ScalarAttributeType.S).build());
            definitions.add(AttributeDefinition.builder().attributeName(MessagePublicationIndex.DUE).attributeType(ScalarAttributeType.N).build());
            definitions.addAll(PrimaryExpiryIndex.attributes());
            indexes.add(MessagePublicationIndex.definition());
            indexes.add(PrimaryExpiryIndex.definition());
        } else {
            definitions.addAll(FollowupDispatchIndex.attributes());
            definitions.addAll(MessageResultInboxIndex.attributes());
            indexes.add(FollowupDispatchIndex.definition());
            indexes.add(MessageResultInboxIndex.definition());
        }
        var requestBuilder = CreateTableRequest.builder()
                .tableName(table)
                .attributeDefinitions(definitions)
                .keySchema(
                        KeySchemaElement.builder().attributeName(PK).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(SK).keyType(KeyType.RANGE).build()
                )
                .globalSecondaryIndexes(indexes)
                .billingMode(BillingMode.PAY_PER_REQUEST);
        CreateTableRequest request = requestBuilder.build();

        dynamoDbClient.createTable(request);
        dynamoDbClient.waiter().waitUntilTableExists(builder -> builder.tableName(table));

        if (enableTimeToLive) enableTimeToLive(table);

        log.info("DynamoDB table created. table={}", table);
    }

    private void enableTimeToLive(String table) {
        var ttl = dynamoDbClient.describeTimeToLive(builder -> builder.tableName(table)).timeToLiveDescription();
        if (ttl != null && (ttl.timeToLiveStatusAsString().equals("ENABLED")
                || ttl.timeToLiveStatusAsString().equals("ENABLING"))) {
            if (!"ttl_epoch_seconds".equals(ttl.attributeName())) {
                throw new IllegalStateException("DynamoDB TTL attribute differs for " + table);
            }
            return;
        }
        dynamoDbClient.updateTimeToLive(builder -> builder.tableName(table)
                .timeToLiveSpecification(TimeToLiveSpecification.builder()
                        .attributeName("ttl_epoch_seconds").enabled(true).build()));
    }

    private void createPublicationIndexIfMissing() {
        var description = dynamoDbClient.describeTable(builder -> builder.tableName(ORIGIN)).table();
        if (description.globalSecondaryIndexes().stream().anyMatch(index -> MessagePublicationIndex.NAME.equals(index.indexName()))) return;
        dynamoDbClient.updateTable(builder -> builder.tableName(ORIGIN)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName(MessagePublicationIndex.BUCKET).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(MessagePublicationIndex.DUE).attributeType(ScalarAttributeType.N).build())
                .globalSecondaryIndexUpdates(GlobalSecondaryIndexUpdate.builder()
                        .create(CreateGlobalSecondaryIndexAction.builder()
                                .indexName(MessagePublicationIndex.NAME)
                                .keySchema(MessagePublicationIndex.definition().keySchema())
                                .projection(MessagePublicationIndex.definition().projection()).build()).build()));
        dynamoDbClient.waiter().waitUntilTableExists(builder -> builder.tableName(ORIGIN));
    }

    private void createPrimaryExpiryIndexIfMissing() {
        var description = dynamoDbClient.describeTable(builder -> builder.tableName(ORIGIN)).table();
        if (description.globalSecondaryIndexes().stream()
                .anyMatch(index -> PrimaryExpiryIndex.NAME.equals(index.indexName()))) return;
        dynamoDbClient.updateTable(builder -> builder.tableName(ORIGIN)
                .attributeDefinitions(PrimaryExpiryIndex.attributes())
                .globalSecondaryIndexUpdates(GlobalSecondaryIndexUpdate.builder()
                        .create(CreateGlobalSecondaryIndexAction.builder()
                                .indexName(PrimaryExpiryIndex.NAME)
                                .keySchema(PrimaryExpiryIndex.definition().keySchema())
                                .projection(PrimaryExpiryIndex.definition().projection()).build()).build()));
        dynamoDbClient.waiter().waitUntilTableExists(builder -> builder.tableName(ORIGIN));
    }

    private void createFollowupIndexIfMissing() {
        var description = dynamoDbClient.describeTable(builder -> builder.tableName(STEP)).table();
        if (description.globalSecondaryIndexes().stream()
                .anyMatch(index -> FollowupDispatchIndex.NAME.equals(index.indexName()))) return;
        dynamoDbClient.updateTable(builder -> builder.tableName(STEP)
                .attributeDefinitions(FollowupDispatchIndex.attributes())
                .globalSecondaryIndexUpdates(GlobalSecondaryIndexUpdate.builder()
                        .create(CreateGlobalSecondaryIndexAction.builder()
                                .indexName(FollowupDispatchIndex.NAME)
                                .keySchema(FollowupDispatchIndex.definition().keySchema())
                                .projection(FollowupDispatchIndex.definition().projection()).build()).build()));
        dynamoDbClient.waiter().waitUntilTableExists(builder -> builder.tableName(STEP));
    }

    private void createResultInboxIndexIfMissing() {
        var description = dynamoDbClient.describeTable(builder -> builder.tableName(STEP)).table();
        if (description.globalSecondaryIndexes().stream()
                .anyMatch(index -> MessageResultInboxIndex.NAME.equals(index.indexName()))) return;
        dynamoDbClient.updateTable(builder -> builder.tableName(STEP)
                .attributeDefinitions(MessageResultInboxIndex.attributes())
                .globalSecondaryIndexUpdates(GlobalSecondaryIndexUpdate.builder()
                        .create(CreateGlobalSecondaryIndexAction.builder()
                                .indexName(MessageResultInboxIndex.NAME)
                                .keySchema(MessageResultInboxIndex.definition().keySchema())
                                .projection(MessageResultInboxIndex.definition().projection()).build()).build()));
        dynamoDbClient.waiter().waitUntilTableExists(builder -> builder.tableName(STEP));
    }

    private boolean tableExists(String table) {
        try {
            dynamoDbClient.describeTable(builder -> builder.tableName(table));
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }
}

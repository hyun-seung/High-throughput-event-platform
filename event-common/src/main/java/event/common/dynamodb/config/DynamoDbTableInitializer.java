package event.common.dynamodb.config;

import lombok.RequiredArgsConstructor;
import event.common.lifecycle.LifecycleIndex;
import event.common.events.EventPublicationIndex;
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

import static event.common.dynamodb.DynamoDbAttributeNames.PK;
import static event.common.dynamodb.DynamoDbAttributeNames.SK;
import static event.common.dynamodb.DynamoDbTableNames.*;

@Slf4j
@RequiredArgsConstructor
public class DynamoDbTableInitializer implements ApplicationRunner {

    private final DynamoDbClient dynamoDbClient;

    @Override
    public void run(ApplicationArguments args) {
        for (String table : java.util.List.of(ORIGIN, STEP)) createIfMissing(table);
    }

    private void createIfMissing(String table) {
        if (tableExists(table)) {
            if (ORIGIN.equals(table)) createPublicationIndexIfMissing();
            log.debug("DynamoDB table already exists. table={}", table);
            return;
        }

        var definitions = new java.util.ArrayList<>(java.util.List.of(
                AttributeDefinition.builder().attributeName(PK).attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName(SK).attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName(LifecycleIndex.BUCKET).attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName(LifecycleIndex.DUE).attributeType(ScalarAttributeType.N).build()));
        var indexes = new java.util.ArrayList<>(java.util.List.of(LifecycleIndex.definition()));
        if (ORIGIN.equals(table)) {
            definitions.add(AttributeDefinition.builder().attributeName(EventPublicationIndex.BUCKET).attributeType(ScalarAttributeType.S).build());
            definitions.add(AttributeDefinition.builder().attributeName(EventPublicationIndex.DUE).attributeType(ScalarAttributeType.N).build());
            indexes.add(EventPublicationIndex.definition());
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

        log.info("DynamoDB table created. table={}", table);
    }

    private void createPublicationIndexIfMissing() {
        var description = dynamoDbClient.describeTable(builder -> builder.tableName(ORIGIN)).table();
        if (description.globalSecondaryIndexes().stream().anyMatch(index -> EventPublicationIndex.NAME.equals(index.indexName()))) return;
        dynamoDbClient.updateTable(builder -> builder.tableName(ORIGIN)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName(EventPublicationIndex.BUCKET).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(EventPublicationIndex.DUE).attributeType(ScalarAttributeType.N).build())
                .globalSecondaryIndexUpdates(GlobalSecondaryIndexUpdate.builder()
                        .create(CreateGlobalSecondaryIndexAction.builder()
                                .indexName(EventPublicationIndex.NAME)
                                .keySchema(EventPublicationIndex.definition().keySchema())
                                .projection(EventPublicationIndex.definition().projection()).build()).build()));
        dynamoDbClient.waiter().waitUntilTableExists(builder -> builder.tableName(ORIGIN));
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

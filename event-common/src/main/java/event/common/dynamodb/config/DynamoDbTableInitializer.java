package event.common.dynamodb.config;

import lombok.RequiredArgsConstructor;
import event.common.lifecycle.LifecycleIndex;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
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
            log.debug("DynamoDB table already exists. table={}", table);
            return;
        }

        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(table)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName(PK).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(SK).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(LifecycleIndex.BUCKET).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(LifecycleIndex.DUE).attributeType(ScalarAttributeType.N).build()
                )
                .keySchema(
                        KeySchemaElement.builder().attributeName(PK).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(SK).keyType(KeyType.RANGE).build()
                )
                .globalSecondaryIndexes(LifecycleIndex.definition())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build();

        dynamoDbClient.createTable(request);
        dynamoDbClient.waiter().waitUntilTableExists(builder -> builder.tableName(table));

        log.info("DynamoDB table created. table={}", table);
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

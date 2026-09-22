package event.common.dynamodb.config;

import lombok.RequiredArgsConstructor;
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
import static event.common.dynamodb.DynamoDbTableNames.DELIVERY_STATE;

@Slf4j
@RequiredArgsConstructor
public class DynamoDbTableInitializer implements ApplicationRunner {

    private final DynamoDbClient dynamoDbClient;

    @Override
    public void run(ApplicationArguments args) {
        if (tableExists()) {
            log.debug("DynamoDB table already exists. table={}", DELIVERY_STATE);
            return;
        }

        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(DELIVERY_STATE)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName(PK).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(SK).attributeType(ScalarAttributeType.S).build()
                )
                .keySchema(
                        KeySchemaElement.builder().attributeName(PK).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(SK).keyType(KeyType.RANGE).build()
                )
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build();

        dynamoDbClient.createTable(request);
        dynamoDbClient.waiter().waitUntilTableExists(builder -> builder.tableName(DELIVERY_STATE));

        log.info("DynamoDB table created. table={}", DELIVERY_STATE);
    }

    private boolean tableExists() {
        try {
            dynamoDbClient.describeTable(builder -> builder.tableName(DELIVERY_STATE));
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }
}

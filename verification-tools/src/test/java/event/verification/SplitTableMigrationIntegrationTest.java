package event.verification;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.*;

@EnabledIfEnvironmentVariable(named = "DYNAMODB_TEST_ENDPOINT", matches = "http://(localhost|127\\.0\\.0\\.1):[0-9]+")
class SplitTableMigrationIntegrationTest {
    private DynamoDbClient db;
    private String source;
    private List<Map<String, AttributeValue>> items;

    @BeforeEach
    void setup() throws Exception {
        db = DynamoDbClient.builder()
                .endpointOverride(LifecycleIndexMigration.localEndpoint(System.getenv("DYNAMODB_TEST_ENDPOINT")))
                .region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"))).build();
        source = "test_split_" + UUID.randomUUID().toString().replace("-", "");
        db.createTable(CreateTableRequest.builder().tableName(source).billingMode(BillingMode.PAY_PER_REQUEST)
                .keySchema(KeySchemaElement.builder().attributeName("pk").keyType("HASH").build(),
                        KeySchemaElement.builder().attributeName("sk").keyType("RANGE").build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.S).build()).build());
        try (var waiter = db.waiter()) { waiter.waitUntilTableExists(request -> request.tableName(source)); }
        SplitTableMigration.ensureTables(db);
        String request = UUID.randomUUID().toString();
        String execution = UUID.randomUUID().toString();
        String receipt = UUID.randomUUID().toString();
        items = List.of(
                Map.of("pk", AttributeValue.fromS("DELIVERY#" + request), "sk", AttributeValue.fromS("META"),
                        "delivery_id", AttributeValue.fromS(execution), "payload", AttributeValue.fromS("{\"test\":true}")),
                Map.of("pk", AttributeValue.fromS("DELIVERY#" + execution), "sk", AttributeValue.fromS("ATTEMPT#" + UUID.randomUUID()),
                        "version", AttributeValue.fromN("1"), "receipt_marker_ids", AttributeValue.builder().ss("b", "a").build()),
                Map.of("pk", AttributeValue.fromS("DELIVERY#" + execution), "sk", AttributeValue.fromS("FINAL"),
                        "publish_state", AttributeValue.fromS("PENDING")),
                Map.of("pk", AttributeValue.fromS("RECEIPT#" + receipt), "sk", AttributeValue.fromS("META"),
                        "delivery_id", AttributeValue.fromS(execution)));
        for (var item : items) db.putItem(requestBuilder -> requestBuilder.tableName(source).item(item));
    }

    @AfterEach
    void cleanup() {
        if (db == null) return;
        try {
            if (items != null) for (var item : items) {
                db.deleteItem(request -> request.tableName(SplitTableMigration.destination(item)).key(key(item)));
            }
            if (source != null) db.deleteTable(request -> request.tableName(source));
        } finally {
            db.close();
        }
    }

    @Test
    void dryRunCopyAndRepeatPreserveSourceAndAllValues() throws Exception {
        var plan = SplitTableMigration.migrate(db, source, false);
        assertEquals(Map.of("ORIGIN", 1, "STEP", 3), plan.get("destinations"));
        assertEquals(4, plan.get("missingBefore"));
        assertEquals(4, SplitTableMigration.migrate(db, source, true).get("missingBefore"));
        assertEquals(0, SplitTableMigration.migrate(db, source, true).get("missingBefore"));
        assertEquals(4, db.scan(request -> request.tableName(source).consistentRead(true)).items().size());
        for (var item : items) {
            var saved = db.getItem(request -> request.tableName(SplitTableMigration.destination(item))
                    .key(key(item)).consistentRead(true)).item();
            assertEquals(SplitTableMigration.canonical(item), SplitTableMigration.canonical(saved));
        }
    }

    @Test
    void conflictNeverOverwritesTargetOrCopiesOtherRows() {
        var changed = new java.util.HashMap<>(items.getFirst());
        changed.put("payload", AttributeValue.fromS("newer-origin"));
        db.putItem(request -> request.tableName("ORIGIN").item(changed));
        assertThrows(IllegalStateException.class, () -> SplitTableMigration.migrate(db, source, true));
        assertEquals(changed, db.getItem(request -> request.tableName("ORIGIN").key(key(changed))).item());
        assertFalse(db.getItem(request -> request.tableName("STEP").key(key(items.get(1)))).hasItem());
    }

    @Test
    void partialCopyAfterLostAcknowledgmentResumesWithoutRewriting() throws Exception {
        var client = mock(DynamoDbClient.class, delegatesTo(db));
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked") Consumer<software.amazon.awssdk.services.dynamodb.model.PutItemRequest.Builder> request = invocation.getArgument(0);
            db.putItem(request);
            throw new IllegalStateException("copy response lost");
        }).when(client).putItem(any(Consumer.class));
        assertThrows(IllegalStateException.class, () -> SplitTableMigration.migrate(client, source, true));
        assertEquals(3, SplitTableMigration.migrate(db, source, true).get("missingBefore"));
        assertEquals(0, SplitTableMigration.migrate(db, source, false).get("missingBefore"));
    }

    private static Map<String, AttributeValue> key(Map<String, AttributeValue> item) {
        return Map.of("pk", item.get("pk"), "sk", item.get("sk"));
    }
}

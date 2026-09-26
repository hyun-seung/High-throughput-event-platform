package event.verification;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SplitTableMigrationTest {
    private static final String SOURCE = "legacy_test";
    private static final List<Map<String, AttributeValue>> ITEMS = List.of(
            item("DELIVERY#request", "META", "delivery_id", AttributeValue.fromS("execution")),
            item("DELIVERY#execution", "ATTEMPT#one", "version", AttributeValue.fromN("1")),
            item("DELIVERY#execution", "FINAL", "publish_state", AttributeValue.fromS("PENDING")),
            item("RECEIPT#receipt", "META", "delivery_id", AttributeValue.fromS("execution")));

    @Test
    void dryRunApplyAndRepeatPreserveEveryFieldAndNeverDeleteSource() throws Exception {
        var fixture = new Fixture();
        var plan = SplitTableMigration.migrate(fixture.db, SOURCE, false);
        assertEquals(4, plan.get("missingBefore"));
        assertEquals(Map.of("ORIGIN", 1, "STEP", 3), plan.get("destinations"));
        assertEquals(false, plan.get("applied"));
        assertEquals(false, plan.get("sourceDeleted"));
        assertEquals(0, fixture.targets.size());
        assertEquals(4, SplitTableMigration.migrate(fixture.db, SOURCE, true).get("missingBefore"));
        assertEquals(0, SplitTableMigration.migrate(fixture.db, SOURCE, true).get("missingBefore"));
        assertEquals(4, fixture.targets.size());
        verify(fixture.db, never()).deleteItem(any(software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest.class));
        verify(fixture.db, never()).deleteTable(any(software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest.class));
        for (var item : ITEMS) {
            String key = fixture.key(SplitTableMigration.destination(item), item);
            assertEquals(SplitTableMigration.canonical(item), SplitTableMigration.canonical(fixture.targets.get(key)));
        }
    }

    @Test
    void conflictStopsBeforeAnyNewCopy() {
        var fixture = new Fixture();
        var changed = new HashMap<>(ITEMS.getFirst());
        changed.put("delivery_id", AttributeValue.fromS("different"));
        fixture.targets.put(fixture.key("ORIGIN", changed), changed);
        assertThrows(IllegalStateException.class, () -> SplitTableMigration.migrate(fixture.db, SOURCE, true));
        assertEquals(1, fixture.targets.size());
    }

    @Test
    void lostAcknowledgmentCanResumeWithoutRewritingCopiedItem() throws Exception {
        var fixture = new Fixture();
        fixture.loseFirstAcknowledgment.set(true);
        assertThrows(IllegalStateException.class, () -> SplitTableMigration.migrate(fixture.db, SOURCE, true));
        assertEquals(1, fixture.targets.size());
        assertEquals(3, SplitTableMigration.migrate(fixture.db, SOURCE, true).get("missingBefore"));
        assertEquals(0, SplitTableMigration.migrate(fixture.db, SOURCE, false).get("missingBefore"));
    }

    @Test
    void rejectsUnknownKeysAndComparesSetsWithoutOrder() {
        assertThrows(IllegalArgumentException.class,
                () -> SplitTableMigration.destination(item("OTHER#one", "META", "x", AttributeValue.fromS("1"))));
        assertThrows(IllegalArgumentException.class,
                () -> SplitTableMigration.migrate(mock(DynamoDbClient.class), "ORIGIN", false));
        var left = item("DELIVERY#one", "META", "markers", AttributeValue.builder().ss("b", "a").build());
        var right = item("DELIVERY#one", "META", "markers", AttributeValue.builder().ss("a", "b").build());
        assertEquals(SplitTableMigration.canonical(left), SplitTableMigration.canonical(right));
    }

    private static Map<String, AttributeValue> item(String pk, String sk, String extra, AttributeValue value) {
        return Map.of("pk", AttributeValue.fromS(pk), "sk", AttributeValue.fromS(sk), extra, value);
    }

    private static class Fixture {
        final DynamoDbClient db = mock(DynamoDbClient.class);
        final Map<String, Map<String, AttributeValue>> targets = new HashMap<>();
        final AtomicBoolean loseFirstAcknowledgment = new AtomicBoolean();

        Fixture() {
            when(db.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder().items(ITEMS).build());
            when(db.describeTable(any(Consumer.class))).thenAnswer(invocation -> {
                var request = DescribeTableRequest.builder();
                ((Consumer<DescribeTableRequest.Builder>) invocation.getArgument(0)).accept(request);
                return DescribeTableResponse.builder().table(TableDescription.builder()
                        .tableName(request.build().tableName())
                        .keySchema(KeySchemaElement.builder().attributeName("pk").keyType("HASH").build(),
                                KeySchemaElement.builder().attributeName("sk").keyType("RANGE").build())
                        .globalSecondaryIndexes(GlobalSecondaryIndexDescription.builder().indexName("lifecycle_due_v1")
                                .indexStatus("ACTIVE")
                                .keySchema(KeySchemaElement.builder().attributeName("lifecycle_bucket").keyType("HASH").build(),
                                        KeySchemaElement.builder().attributeName("lifecycle_due").keyType("RANGE").build())
                                .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build()).build())
                        .build()).build();
            });
            when(db.getItem(any(Consumer.class))).thenAnswer(invocation -> {
                var builder = GetItemRequest.builder();
                ((Consumer<GetItemRequest.Builder>) invocation.getArgument(0)).accept(builder);
                var request = builder.build();
                var item = targets.get(key(request.tableName(), request.key()));
                return item == null ? GetItemResponse.builder().build() : GetItemResponse.builder().item(item).build();
            });
            when(db.putItem(any(Consumer.class))).thenAnswer(invocation -> {
                var builder = PutItemRequest.builder();
                ((Consumer<PutItemRequest.Builder>) invocation.getArgument(0)).accept(builder);
                var request = builder.build();
                String key = key(request.tableName(), request.item());
                assertEquals("attribute_not_exists(pk) AND attribute_not_exists(sk)", request.conditionExpression());
                assertFalse(targets.containsKey(key));
                targets.put(key, request.item());
                if (loseFirstAcknowledgment.compareAndSet(true, false)) throw new IllegalStateException("ack lost");
                return PutItemResponse.builder().build();
            });
        }

        String key(String table, Map<String, AttributeValue> item) {
            return table + "/" + item.get("pk").s() + "/" + item.get("sk").s();
        }
    }
}

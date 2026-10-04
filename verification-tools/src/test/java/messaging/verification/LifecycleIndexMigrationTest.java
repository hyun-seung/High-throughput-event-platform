package messaging.verification;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class LifecycleIndexMigrationTest {
    private static final String ID = "00000000-0000-0000-0000-000000000001";

    @Test
    void dryRunOnlyDescribesBothTables() throws Exception {
        var db = mock(DynamoDbClient.class);
        when(db.describeTable(any(Consumer.class))).thenReturn(described(false));
        var output = new ArrayList<String>();
        LifecycleIndexMigration.migrate(db, false, List.of(ID), output::add);
        assertEquals(2, output.size());
        assertTrue(output.getFirst().contains("\"indexExists\":false"));
        assertTrue(output.getLast().contains("\"table\":\"STEP\""));
        verify(db, never()).updateTable(any(UpdateTableRequest.class));
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
        verify(db, never()).query(any(software.amazon.awssdk.services.dynamodb.model.QueryRequest.class));
    }

    @Test
    void applyCreatesIndexAndBackfillsOnlyExplicitId() throws Exception {
        var db = mock(DynamoDbClient.class);
        var descriptions = new AtomicInteger();
        when(db.describeTable(any(Consumer.class))).thenAnswer(ignored -> described(descriptions.incrementAndGet() % 2 == 0));
        when(db.query(any(software.amazon.awssdk.services.dynamodb.model.QueryRequest.class)))
                .thenReturn(QueryResponse.builder().items(meta()).build());
        var output = new ArrayList<String>();
        LifecycleIndexMigration.migrate(db, true, List.of(ID), output::add);
        assertEquals(4, output.size());
        assertEquals("backfilled " + ID, output.get(1));
        verify(db, times(2)).updateTable(any(UpdateTableRequest.class));
        verify(db, times(2)).updateItem(any(UpdateItemRequest.class));
        verify(db, times(2)).query(any(software.amazon.awssdk.services.dynamodb.model.QueryRequest.class));
    }

    @Test
    void protectsLocalEndpointAndPreservesConditionalBackfillRules() {
        assertThrows(IllegalArgumentException.class, () -> LifecycleIndexMigration.localEndpoint("https://localhost:18000"));
        assertThrows(IllegalArgumentException.class, () -> LifecycleIndexMigration.localEndpoint("http://example.com:18000"));
        assertEquals("lifecycle-v1-1", LifecycleIndexMigration.bucket(ID));
        assertThrows(IllegalArgumentException.class, () -> LifecycleIndexMigration.bucket("한글"));

        var meta = meta();
        assertEquals("0", LifecycleIndexMigration.plan("ORIGIN", ID, meta, false).expressionAttributeValues().get(":due").n());
        assertNull(LifecycleIndexMigration.plan("ORIGIN", ID, meta, true));
        var pending = Map.of("pk", AttributeValue.fromS("DELIVERY#" + ID), "sk", AttributeValue.fromS("ATTEMPT#one"),
                "publish_state", AttributeValue.fromS("PENDING"), "result_event", AttributeValue.fromS("event"),
                "version", AttributeValue.fromN("2"));
        var pendingUpdate = LifecycleIndexMigration.plan("STEP", ID, pending, true);
        assertEquals("0", pendingUpdate.expressionAttributeValues().get(":due").n());
        assertTrue(pendingUpdate.conditionExpression().contains("result_event = :event"));
        assertEquals("version", pendingUpdate.expressionAttributeNames().get("#version"));
        var active = Map.of("pk", AttributeValue.fromS("DELIVERY#" + ID), "sk", AttributeValue.fromS("ATTEMPT#one"),
                "status", AttributeValue.fromS("PROCESSING"), "version", AttributeValue.fromN("2"),
                "next_attempt_at", AttributeValue.fromN("123"));
        var activeUpdate = LifecycleIndexMigration.plan("STEP", ID, active, true);
        assertEquals("123", activeUpdate.expressionAttributeValues().get(":due").n());
        assertTrue(activeUpdate.conditionExpression().contains("attribute_not_exists(lifecycle_closed)"));
        var closed = new java.util.HashMap<>(active);
        closed.put("lifecycle_closed", AttributeValue.fromBool(true));
        assertNull(LifecycleIndexMigration.plan("STEP", ID, closed, true));
        var unknown = new java.util.HashMap<>(active);
        unknown.put("status", AttributeValue.fromS("UNKNOWN"));
        assertThrows(IllegalArgumentException.class, () -> LifecycleIndexMigration.plan("STEP", ID, unknown, true));
        var complete = new java.util.HashMap<>(active);
        complete.put("completion_event_id", AttributeValue.fromS("done"));
        assertNull(LifecycleIndexMigration.plan("STEP", ID, complete, true));
    }

    private static Map<String, AttributeValue> meta() {
        return Map.of("pk", AttributeValue.fromS("DELIVERY#" + ID), "sk", AttributeValue.fromS("META"));
    }

    private static DescribeTableResponse described(boolean indexed) {
        var table = TableDescription.builder();
        if (indexed) table.globalSecondaryIndexes(GlobalSecondaryIndexDescription.builder()
                .indexName("lifecycle_due_v1").indexStatus("ACTIVE").build());
        return DescribeTableResponse.builder().table(table.build()).build();
    }
}

package messaging.verification;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DynamoDbBenchmarkTest {
    private static final String TABLE = "platform_sdk_bench_" + "a".repeat(32);

    @ParameterizedTest
    @ValueSource(strings = {"https://dynamodb.ap-northeast-2.amazonaws.com", "http://example.org:8000",
            "http://user@localhost:8000", "http://localhost:8000/path", "http://localhost:8000/?query=1"})
    void rejectsNonLocalOrAmbiguousEndpoints(String endpoint) {
        assertThrows(IllegalArgumentException.class, () -> DynamoDbBenchmark.Settings.parse(
                new String[] {"--endpoint", endpoint, "--output", "report.json"}));
    }

    @Test
    void rejectsUnboundedOrMistypedOptions() {
        for (String[] extra : List.of(new String[]{"--cases", "129:64"}, new String[]{"--cases", "1:0"},
                new String[]{"--seconds", "100"}, new String[]{"--rounds", "100"},
                new String[]{"--profiless", "conditional-put"})) {
            assertThrows(IllegalArgumentException.class, () -> DynamoDbBenchmark.Settings.parse(
                    new String[]{"--endpoint", "http://localhost:8000", "--output", "report.json", extra[0], extra[1]}));
        }
    }

    @Test
    void aCreateConflictCannotDeleteAnExistingTableOrRunWork() throws Exception {
        var db = mock(DynamoDbClient.class);
        when(db.createTable(any(CreateTableRequest.class)))
                .thenThrow(ResourceInUseException.builder().message("exists").build());
        var ran = new java.util.concurrent.atomic.AtomicBoolean();

        assertThrows(ResourceInUseException.class, () -> DynamoDbBenchmark.withOwnedTable(db, TABLE, () -> {
            ran.set(true); return null;
        }));

        assertFalse(ran.get());
        verify(db, never()).deleteTable(any(DeleteTableRequest.class));
    }

    @Test
    void cleansUpOnlyItsCreatedTableEvenWhenWorkFails() {
        var db = mock(DynamoDbClient.class);
        when(db.describeTable(any(DescribeTableRequest.class))).thenReturn(DescribeTableResponse.builder()
                .table(TableDescription.builder().tableStatus(TableStatus.ACTIVE).build()).build());
        var failure = new IllegalStateException("phase failed");

        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> DynamoDbBenchmark.withOwnedTable(db, TABLE, () -> { throw failure; })));

        var order = inOrder(db);
        order.verify(db).createTable(argThat((CreateTableRequest request) -> TABLE.equals(request.tableName())));
        order.verify(db).describeTable(any(DescribeTableRequest.class));
        order.verify(db).deleteTable(argThat((DeleteTableRequest request) -> TABLE.equals(request.tableName())));
    }

    @Test
    void refusesBusinessTableNamesBeforeAnyIo() {
        var db = mock(DynamoDbClient.class);
        assertThrows(IllegalArgumentException.class, () -> DynamoDbBenchmark.withOwnedTable(db, "ORIGIN", () -> null));
        verifyNoInteractions(db);
    }

    @Test
    void resultCycleKeepsTheDeclaredSixCallsAndConditionalAckOrder() {
        var db = mock(DynamoDbClient.class);
        DynamoDbBenchmark.execute(db, TABLE, DynamoDbBenchmark.Profile.RESULT_CYCLE, "inbox-1", "origin-1", "step-1");
        var order = inOrder(db);
        order.verify(db).putItem(argThat((PutItemRequest request) -> TABLE.equals(request.tableName())
                && "inbox-1".equals(request.item().get("pk").s())
                && "attribute_not_exists(pk)".equals(request.conditionExpression())));
        order.verify(db).getItem(argThat((GetItemRequest request) -> request.consistentRead()
                && "origin-1".equals(request.key().get("pk").s())));
        order.verify(db).getItem(argThat((GetItemRequest request) -> request.consistentRead()
                && "step-1".equals(request.key().get("pk").s())));
        order.verify(db).updateItem(argThat((UpdateItemRequest request) -> "origin-1".equals(request.key().get("pk").s())
                && "attribute_exists(pk)".equals(request.conditionExpression())));
        order.verify(db).updateItem(argThat((UpdateItemRequest request) -> "record_id = :id".equals(request.conditionExpression())
                && "inbox-1".equals(request.expressionAttributeValues().get(":id").s())));
        order.verify(db).updateItem(argThat((UpdateItemRequest request) -> "inbox-1".equals(request.key().get("pk").s())
                && "DONE".equals(request.expressionAttributeValues().get(":done").s())));
        order.verifyNoMoreInteractions();
        assertEquals(6, DynamoDbBenchmark.Profile.RESULT_CYCLE.logicalCalls);
    }

    @Test
    void reportsMeasuredRecordDurationsRatherThanInverseThroughput() {
        var timing = DynamoDbBenchmark.latencies(List.of(2_000_000L, 8_000_000L));
        assertEquals(5d, timing.get("meanSuccessfulRecordMs"));
        assertEquals(8d, timing.get("p95SuccessfulRecordMs"));
    }
}

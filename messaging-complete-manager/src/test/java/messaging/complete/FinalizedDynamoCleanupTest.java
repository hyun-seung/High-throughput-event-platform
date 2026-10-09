package messaging.complete;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class FinalizedDynamoCleanupTest {
    private static final String ID = "a".repeat(32);
    private static final String DECISION = "decision-1";
    private static final Instant NOW = Instant.parse("2026-10-08T00:00:00Z");
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final FinalizedDynamoCleanup cleanup = new FinalizedDynamoCleanup(
            jdbc, db, Clock.fixed(NOW, ZoneOffset.UTC), 25, 4, Duration.ofDays(7));
    private final FinalizedDynamoCleanup.Pending item =
            new FinalizedDynamoCleanup.Pending(ID, DECISION, "PRIMARY");

    @BeforeEach
    void batchWritesSucceed() {
        when(db.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(BatchWriteItemResponse.builder().build());
    }

    @Test
    void waitsForCustomerWebhookHandoffBeforeDeletingAnything() {
        when(db.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
            GetItemRequest request = call.getArgument(0);
            return GetItemResponse.builder().item(request.tableName().equals("ORIGIN")
                    ? origin() : Map.of("status", s("FINALIZED_PUBLISHED"))).build();
        });

        assertFalse(cleanup.cleanup(item));

        verify(db, never()).query(any(QueryRequest.class));
        verify(db, never()).batchWriteItem(any(BatchWriteItemRequest.class));
        verify(db, never()).deleteItem(any(DeleteItemRequest.class));
    }

    @Test
    void marksTtlAndDeletesStepsBeforeOriginAfterHandoffs() {
        when(db.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
            GetItemRequest request = call.getArgument(0);
            return GetItemResponse.builder().item(request.tableName().equals("ORIGIN")
                    ? origin() : Map.of("status", s("PUBLISHED"))).build();
        });
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(Map.of("sk", s("PRIMARY_DECISION#" + DECISION)),
                        Map.of("sk", s("RESULT_INBOX#result-1"))).build());

        assertTrue(cleanup.cleanup(item));

        var updates = org.mockito.ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(db, times(4)).updateItem(updates.capture());
        long expiresAt = NOW.plus(Duration.ofDays(7)).getEpochSecond();
        for (int index = 1; index < 4; index++) {
            assertEquals(Long.toString(expiresAt), updates.getAllValues().get(index)
                    .expressionAttributeValues().get(":ttl").n());
        }
        var batches = org.mockito.ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        var order = inOrder(db);
        order.verify(db, times(4)).updateItem(any(UpdateItemRequest.class));
        order.verify(db).batchWriteItem(batches.capture());
        order.verify(db).deleteItem(argThat((DeleteItemRequest request) -> request.tableName().equals("ORIGIN")));
        assertEquals(List.of("PRIMARY_DECISION#" + DECISION, "RESULT_INBOX#result-1"),
                batches.getValue().requestItems().get("STEP").stream()
                        .map(write -> write.deleteRequest().key().get("sk").s()).toList());
        verify(db, never()).deleteItem(argThat((DeleteItemRequest request) -> request.tableName().equals("STEP")));
    }

    @Test
    void resumesPartialCleanupAfterOriginWasDeleted() {
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(Map.of("sk", s("RESULT_INBOX#late"))).build());

        assertTrue(cleanup.cleanup(item));

        verify(db).batchWriteItem(argThat((BatchWriteItemRequest request) ->
                request.requestItems().get("STEP").getFirst().deleteRequest().key().get("sk").s()
                        .equals("RESULT_INBOX#late")));
        verify(db, never()).deleteItem(argThat((DeleteItemRequest request) -> request.tableName().equals("ORIGIN")));
    }

    @Test
    void splitsDeletesIntoBatchesOfAtMost25AcrossQueryPages() {
        readyOrigin();
        var first = IntStream.range(0, 25).mapToObj(i -> Map.of("sk", s("STEP#" + i))).toList();
        var second = IntStream.range(25, 50).mapToObj(i -> Map.of("sk", s("STEP#" + i))).toList();
        var third = IntStream.range(50, 53).mapToObj(i -> Map.of("sk", s("STEP#" + i))).toList();
        var cursor = Map.of("pk", s("DELIVERY#" + ID), "sk", s("STEP#24"));
        var nextCursor = Map.of("pk", s("DELIVERY#" + ID), "sk", s("STEP#49"));
        when(db.query(any(QueryRequest.class))).thenReturn(
                QueryResponse.builder().items(first).lastEvaluatedKey(cursor).build(),
                QueryResponse.builder().items(second).lastEvaluatedKey(nextCursor).build(),
                QueryResponse.builder().items(third).build());

        assertTrue(cleanup.cleanup(item));

        var queries = org.mockito.ArgumentCaptor.forClass(QueryRequest.class);
        verify(db, times(3)).query(queries.capture());
        assertEquals(cursor, queries.getAllValues().get(1).exclusiveStartKey());
        assertEquals(nextCursor, queries.getAllValues().get(2).exclusiveStartKey());
        var batches = org.mockito.ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        verify(db, times(3)).batchWriteItem(batches.capture());
        assertEquals(List.of(25, 25, 3), batches.getAllValues().stream()
                .map(request -> request.requestItems().get("STEP").size()).toList());
        var deleted = batches.getAllValues().stream().flatMap(request -> request.requestItems().get("STEP").stream())
                .map(write -> write.deleteRequest().key().get("sk").s()).toList();
        assertEquals(IntStream.range(0, 53).mapToObj(i -> "STEP#" + i).toList(), deleted);
        var order = inOrder(db);
        order.verify(db, times(55)).updateItem(any(UpdateItemRequest.class));
        order.verify(db, times(3)).batchWriteItem(any(BatchWriteItemRequest.class));
        order.verify(db).deleteItem(any(DeleteItemRequest.class));
    }

    @Test
    void retainsOriginWhenBatchHasUnprocessedDeletesAndResumesRemainingSteps() {
        readyOrigin();
        var remaining = Map.of("pk", s("DELIVERY#" + ID), "sk", s("STEP#remaining"));
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(Map.of("sk", s("STEP#done")), remaining).build(),
                QueryResponse.builder().items(remaining).build());
        when(db.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(
                BatchWriteItemResponse.builder().unprocessedItems(Map.of("STEP", List.of(
                        WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(remaining).build()).build())))
                        .build(), BatchWriteItemResponse.builder().build());

        assertFalse(cleanup.cleanup(item));
        verify(db, never()).deleteItem(any(DeleteItemRequest.class));
        assertTrue(cleanup.cleanup(item));

        var batches = org.mockito.ArgumentCaptor.forClass(BatchWriteItemRequest.class);
        verify(db, times(2)).batchWriteItem(batches.capture());
        assertEquals(List.of(remaining), batches.getAllValues().getLast().requestItems().get("STEP").stream()
                .map(write -> write.deleteRequest().key()).toList());
        verify(db).deleteItem(argThat((DeleteItemRequest request) -> request.tableName().equals("ORIGIN")));
    }

    @Test
    void retainsOriginAndTtlProtectionWhenBatchCallFails() {
        readyOrigin();
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(Map.of("sk", s("STEP#one"))).build());
        when(db.batchWriteItem(any(BatchWriteItemRequest.class))).thenThrow(new IllegalStateException("unavailable"));

        assertThrows(IllegalStateException.class, () -> cleanup.cleanup(item));

        verify(db, never()).deleteItem(any(DeleteItemRequest.class));
        verify(db, times(2)).updateItem(argThat((UpdateItemRequest request) ->
                request.expressionAttributeValues().containsKey(":ttl")));
    }

    @Test
    void deletesOriginWithoutAnEmptyBatchWhenNoStepsRemain() {
        readyOrigin();
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());

        assertTrue(cleanup.cleanup(item));

        verify(db, never()).batchWriteItem(any(BatchWriteItemRequest.class));
        verify(db).deleteItem(argThat((DeleteItemRequest request) -> request.tableName().equals("ORIGIN")));
    }

    private void readyOrigin() {
        when(db.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
            GetItemRequest request = call.getArgument(0);
            return GetItemResponse.builder().item(request.tableName().equals("ORIGIN")
                    ? origin() : Map.of("status", s("PUBLISHED"))).build();
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void drainsFullPagesWithoutWaitingAndStopsAfterAShortPage() {
        var worker = spy(cleanup);
        doReturn(true).when(worker).cleanup(any());
        var fullPage = IntStream.range(0, 25)
                .mapToObj(i -> new FinalizedDynamoCleanup.Pending("id-" + i, DECISION, "PRIMARY")).toList();
        when(jdbc.query(anyString(), any(RowMapper.class), eq(25)))
                .thenReturn(fullPage, fullPage, List.of(item));

        worker.poll();

        verify(worker, times(51)).cleanup(any());
        verify(jdbc, times(3)).query(anyString(), any(RowMapper.class), eq(25));
        verify(jdbc, times(51)).update(contains("cleanup_status = 'DONE'"), anyString());
    }

    @Test
    @SuppressWarnings("unchecked")
    void limitsEachPollEvenWhenEveryPageIsFull() {
        var worker = spy(cleanup);
        doReturn(true).when(worker).cleanup(any());
        var fullPage = IntStream.range(0, 25)
                .mapToObj(i -> new FinalizedDynamoCleanup.Pending("id-" + i, DECISION, "PRIMARY")).toList();
        when(jdbc.query(anyString(), any(RowMapper.class), eq(25))).thenReturn(fullPage);

        worker.poll();

        verify(jdbc, times(4)).query(anyString(), any(RowMapper.class), eq(25));
        verify(worker, times(100)).cleanup(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void leavesPartialAndFailedCleanupPendingForTheExistingRetrySchedule() {
        var worker = spy(cleanup);
        var failed = new FinalizedDynamoCleanup.Pending("failed", DECISION, "PRIMARY");
        doReturn(false).when(worker).cleanup(item);
        doThrow(new IllegalStateException("unavailable")).when(worker).cleanup(failed);
        when(jdbc.query(anyString(), any(RowMapper.class), eq(25))).thenReturn(List.of(item, failed));

        worker.poll();

        verify(jdbc, never()).update(contains("cleanup_status = 'DONE'"), anyString());
        verify(jdbc).update(contains("interval '10 seconds'"), eq(ID));
        verify(jdbc).update(contains("cleanup_attempts = cleanup_attempts + 1"), eq("IllegalStateException"), eq("failed"));
    }

    @Test
    void rejectsUnboundedOrDisabledPageLimits() {
        for (int limit : new int[] {0, 21}) {
            assertThrows(IllegalArgumentException.class, () -> new FinalizedDynamoCleanup(
                    jdbc, db, Clock.fixed(NOW, ZoneOffset.UTC), 25, limit, Duration.ofDays(7)));
        }
    }

    private static Map<String, AttributeValue> origin() {
        return Map.of("status", s("PRIMARY_SUCCEEDED"), "primary_decision_id", s(DECISION));
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

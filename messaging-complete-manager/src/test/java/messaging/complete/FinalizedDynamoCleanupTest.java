package messaging.complete;

import org.junit.jupiter.api.Test;
import messaging.common.messages.MessageOriginCodec;
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
    private final io.micrometer.core.instrument.simple.SimpleMeterRegistry metrics =
            new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
    private static final String ID = "a".repeat(32);
    private static final String DECISION = "decision-1";
    private static final Instant NOW = Instant.parse("2026-10-08T00:00:00Z");
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final FinalizedDynamoCleanup cleanup = new FinalizedDynamoCleanup(
            jdbc, db, Clock.fixed(NOW, ZoneOffset.UTC), 25, 4, Duration.ofDays(7), metrics, 1);
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
                    ? Map.of("status", s("PRIMARY_SUCCEEDED"), "primary_decision_id", s(DECISION)) : Map.of()).build();
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
                .items(Map.of("sk", s("HTTP_SEND#" + DECISION)),
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
        assertEquals(List.of("HTTP_SEND#" + DECISION, "RESULT_INBOX#result-1"),
                batches.getValue().requestItems().get("STEP").stream()
                        .map(write -> write.deleteRequest().key().get("sk").s()).toList());
        verify(db, never()).deleteItem(argThat((DeleteItemRequest request) -> request.tableName().equals("STEP")));
    }

    @Test
    void ackForAnotherDecisionCannotPermitCleanup() {
        var origin = new java.util.HashMap<>(origin());
        origin.put(MessageOriginCodec.FINAL_KAFKA_ACK_DECISION_ID, s("another-decision"));
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(origin).build());
        assertFalse(cleanup.cleanup(item));
        verify(db, never()).query(any(QueryRequest.class));
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void secondaryCleanupRequiresSecondaryFinalAck() {
        var secondary = new FinalizedDynamoCleanup.Pending(ID, "secondary-result", "SECONDARY");
        var origin = new java.util.HashMap<String, AttributeValue>();
        origin.put("status", s("SECONDARY_SUCCEEDED"));
        origin.put("secondary_decision_id", s(secondary.decisionId()));
        origin.put("primary_kafka_ack_decision_id", s(DECISION));
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(origin).build());
        assertFalse(cleanup.cleanup(secondary));
        origin.put(MessageOriginCodec.FINAL_KAFKA_ACK_DECISION_ID, s(secondary.decisionId()));
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(origin).build());
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());
        assertTrue(cleanup.cleanup(secondary));
        verify(db).deleteItem(any(DeleteItemRequest.class));
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
        assertEquals(1, metrics.get("messaging.scheduled.work.duration").tag("phase", "poll").timer().count());
        assertEquals(3, metrics.get("messaging.scheduled.work.duration").tag("phase", "query").timer().count());
        assertEquals(51, metrics.get("messaging.scheduled.work.duration").tag("phase", "item").timer().count());
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
    @SuppressWarnings("unchecked")
    void boundsParallelMessagesAndWaitsForThePageBeforeReadingAnother() throws Exception {
        var worker = spy(new FinalizedDynamoCleanup(jdbc, db, Clock.fixed(NOW, ZoneOffset.UTC),
                4, 4, Duration.ofDays(7), metrics, 2));
        var page = IntStream.range(0, 4).mapToObj(i ->
                new FinalizedDynamoCleanup.Pending("parallel-" + i, DECISION, "PRIMARY")).toList();
        when(jdbc.query(anyString(), any(RowMapper.class), eq(4))).thenReturn(page, List.of());
        var entered = new java.util.concurrent.CountDownLatch(2);
        var release = new java.util.concurrent.CountDownLatch(1);
        var active = new java.util.concurrent.atomic.AtomicInteger();
        var maximum = new java.util.concurrent.atomic.AtomicInteger();
        var seen = java.util.concurrent.ConcurrentHashMap.<String>newKeySet();
        doAnswer(call -> {
            FinalizedDynamoCleanup.Pending pending = call.getArgument(0);
            assertTrue(seen.add(pending.clientMsgId()), "Same message started twice");
            maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
            entered.countDown();
            try {
                assertTrue(release.await(10, java.util.concurrent.TimeUnit.SECONDS));
                if (pending.clientMsgId().equals("parallel-0")) return false;
                if (pending.clientMsgId().equals("parallel-1")) throw new IllegalStateException("unavailable");
                return true;
            } finally { active.decrementAndGet(); }
        }).when(worker).cleanup(any());
        try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            var poll = executor.submit(worker::poll);
            try {
                assertTrue(entered.await(10, java.util.concurrent.TimeUnit.SECONDS));
                assertEquals(2, active.get());
                verify(jdbc, times(1)).query(anyString(), any(RowMapper.class), eq(4));
                assertFalse(poll.isDone());
            } finally { release.countDown(); }
            poll.get(10, java.util.concurrent.TimeUnit.SECONDS);
        }
        assertEquals(2, maximum.get());
        assertEquals(4, seen.size());
        verify(jdbc, times(2)).query(anyString(), any(RowMapper.class), eq(4));
        verify(jdbc, times(2)).update(contains("cleanup_status = 'DONE'"), anyString());
        verify(jdbc).update(contains("interval '10 seconds'"), eq("parallel-0"));
        verify(jdbc).update(contains("cleanup_attempts = cleanup_attempts + 1"), eq("IllegalStateException"), eq("parallel-1"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void interruptedPollStopsNewMessagesAndDrainsItsWorkerThreads() throws Exception {
        var worker = spy(new FinalizedDynamoCleanup(jdbc, db, Clock.fixed(NOW, ZoneOffset.UTC),
                4, 4, Duration.ofDays(7), metrics, 2));
        var page = IntStream.range(0, 4).mapToObj(i ->
                new FinalizedDynamoCleanup.Pending("interrupted-" + i, DECISION, "PRIMARY")).toList();
        when(jdbc.query(anyString(), any(RowMapper.class), eq(4))).thenReturn(page);
        var entered = new java.util.concurrent.CountDownLatch(2);
        var release = new java.util.concurrent.CountDownLatch(1);
        var threads = java.util.concurrent.ConcurrentHashMap.<Thread>newKeySet();
        doAnswer(call -> {
            threads.add(Thread.currentThread());
            entered.countDown();
            try { release.await(); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            return false;
        }).when(worker).cleanup(any());
        var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var interruptedAtExit = new java.util.concurrent.atomic.AtomicBoolean();
        var poll = new Thread(() -> {
            try { worker.poll(); }
            catch (Throwable error) { failure.set(error); }
            finally { interruptedAtExit.set(Thread.currentThread().isInterrupted()); }
        });
        poll.start();
        try {
            assertTrue(entered.await(10, java.util.concurrent.TimeUnit.SECONDS));
            poll.interrupt();
            poll.join(10000);
            assertFalse(poll.isAlive());
            assertInstanceOf(IllegalStateException.class, failure.get());
            assertTrue(interruptedAtExit.get());
            verify(worker, times(2)).cleanup(any());
            verify(jdbc, times(1)).query(anyString(), any(RowMapper.class), eq(4));
            verify(jdbc, never()).update(contains("cleanup_status = 'DONE'"), anyString());
            assertTrue(threads.stream().noneMatch(Thread::isAlive));
        } finally {
            release.countDown();
            poll.interrupt();
            poll.join(10000);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void interruptedPollKeepsItsLockUntilAnUninterruptibleWorkerHasFinished() throws Exception {
        var worker = spy(new FinalizedDynamoCleanup(jdbc, db, Clock.fixed(NOW, ZoneOffset.UTC),
                4, 4, Duration.ofDays(7), metrics, 2));
        var page = IntStream.range(0, 4).mapToObj(i ->
                new FinalizedDynamoCleanup.Pending("driver-" + i, DECISION, "PRIMARY")).toList();
        when(jdbc.query(anyString(), any(RowMapper.class), eq(4))).thenReturn(page, List.of());
        var entered = new java.util.concurrent.CountDownLatch(2);
        var release = new java.util.concurrent.CountDownLatch(1);
        doAnswer(call -> {
            entered.countDown();
            boolean done = false;
            while (!done) {
                try { release.await(); done = true; }
                catch (InterruptedException ignored) { /* Emulate a driver that finishes despite cancellation. */ }
            }
            return true;
        }).when(worker).cleanup(any());
        var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var first = new Thread(() -> {
            try { worker.poll(); } catch (Throwable error) { failure.set(error); }
        });
        var second = new Thread(worker::poll);
        first.start();
        try {
            assertTrue(entered.await(10, java.util.concurrent.TimeUnit.SECONDS));
            first.interrupt();
            second.start();
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
            while (second.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertEquals(Thread.State.BLOCKED, second.getState());
            assertTrue(first.isAlive());
            verify(jdbc, times(1)).query(anyString(), any(RowMapper.class), eq(4));
        } finally {
            release.countDown();
            first.join(10000);
            second.join(10000);
        }
        assertFalse(first.isAlive());
        assertFalse(second.isAlive());
        assertInstanceOf(IllegalStateException.class, failure.get());
        verify(worker, times(2)).cleanup(any());
        verify(jdbc, times(2)).query(anyString(), any(RowMapper.class), eq(4));
    }

    @Test
    void rejectsUnboundedCleanupConcurrency() {
        for (int concurrency : new int[] {0, 5}) {
            assertThrows(IllegalArgumentException.class, () -> new FinalizedDynamoCleanup(
                    jdbc, db, Clock.fixed(NOW, ZoneOffset.UTC), 25, 4, Duration.ofDays(7), metrics, concurrency));
        }
    }

    @Test
    void rejectsUnboundedOrDisabledPageLimits() {
        for (int limit : new int[] {0, 21}) {
            assertThrows(IllegalArgumentException.class, () -> new FinalizedDynamoCleanup(
                    jdbc, db, Clock.fixed(NOW, ZoneOffset.UTC), 25, limit, Duration.ofDays(7), metrics, 1));
        }
    }

    private static Map<String, AttributeValue> origin() {
        return Map.of("status", s("PRIMARY_SUCCEEDED"), "primary_decision_id", s(DECISION),
                MessageOriginCodec.FINAL_KAFKA_ACK_DECISION_ID, s(DECISION));
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

package messaging.result;

import messaging.common.messages.PrimaryExpiryIndex;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PrimaryExpiryBackfillTest {
    private static final String ID = "a".repeat(32);
    private static final Instant RECEIVED = Instant.parse("2026-10-07T00:00:00Z");

    @Test
    void activeOldOriginReceivesThreeHourIndexWithConditionalWrite() {
        var plan = PrimaryExpiryBackfill.plan(oldOrigin());

        assertNotNull(plan);
        assertEquals(PrimaryExpiryIndex.bucket(ID), plan.expressionAttributeValues().get(":bucket").s());
        assertEquals(Long.toString(RECEIVED.plus(PrimaryExpiryIndex.TTL).toEpochMilli()),
                plan.expressionAttributeValues().get(":deadline").n());
        assertTrue(plan.conditionExpression().contains("attribute_not_exists(#bucket)"));
        assertTrue(plan.conditionExpression().contains("#status = :received"));
    }

    @Test
    void completedAndAlreadyIndexedMessagesAreSkippedButPartialIndexIsRejected() {
        var closed = oldOrigin();
        closed.put("status", s("PRIMARY_SUCCEEDED"));
        assertNull(PrimaryExpiryBackfill.plan(closed));

        var indexed = oldOrigin();
        indexed.put(PrimaryExpiryIndex.BUCKET, s(PrimaryExpiryIndex.bucket(ID)));
        assertThrows(IllegalStateException.class, () -> PrimaryExpiryBackfill.plan(indexed));
        indexed.put(PrimaryExpiryIndex.DUE, n(RECEIVED.plus(PrimaryExpiryIndex.TTL).toEpochMilli()));
        indexed.put(PrimaryExpiryIndex.DEADLINE, n(RECEIVED.plus(PrimaryExpiryIndex.TTL).toEpochMilli()));
        assertNull(PrimaryExpiryBackfill.plan(indexed));
    }

    @Test
    void scansOneBoundedPageAtATimeAndStopsAfterTheLastPage() {
        var db = mock(DynamoDbClient.class);
        var cursor = Map.of("pk", s("DELIVERY#cursor"), "sk", s("META"));
        when(db.scan(any(ScanRequest.class))).thenReturn(
                ScanResponse.builder().items(oldOrigin()).lastEvaluatedKey(cursor).build(),
                ScanResponse.builder().build());
        var backfill = new PrimaryExpiryBackfill(db, 25);

        backfill.poll();
        backfill.poll();
        backfill.poll();

        var scans = org.mockito.ArgumentCaptor.forClass(ScanRequest.class);
        verify(db, times(2)).scan(scans.capture());
        assertEquals(25, scans.getAllValues().get(0).limit());
        assertFalse(scans.getAllValues().get(0).hasExclusiveStartKey());
        assertEquals(cursor, scans.getAllValues().get(1).exclusiveStartKey());
        verify(db).updateItem(any(software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest.class));
    }

    @Test
    void failedPageIsRetriedWithoutAdvancingTheCursor() {
        var db = mock(DynamoDbClient.class);
        when(db.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder().items(oldOrigin()).build());
        when(db.updateItem(any(UpdateItemRequest.class))).thenThrow(new IllegalStateException("DynamoDB unavailable"))
                .thenReturn(UpdateItemResponse.builder().build());
        var backfill = new PrimaryExpiryBackfill(db, 25);

        assertThrows(IllegalStateException.class, backfill::poll);
        backfill.poll();

        var scans = org.mockito.ArgumentCaptor.forClass(ScanRequest.class);
        verify(db, times(2)).scan(scans.capture());
        assertEquals(scans.getAllValues().get(0).exclusiveStartKey(),
                scans.getAllValues().get(1).exclusiveStartKey());
        verify(db, times(2)).updateItem(any(UpdateItemRequest.class));
    }

    private static Map<String, AttributeValue> oldOrigin() {
        var item = new HashMap<String, AttributeValue>();
        item.put("pk", s("DELIVERY#" + ID));
        item.put("sk", s("META"));
        item.put("schema_version", n(4));
        item.put("status", s("RECEIVED"));
        item.put("delivery_id", s(ID));
        item.put("message_id", s("customer-message"));
        item.put("occurred_at", s(RECEIVED.toString()));
        return item;
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
    private static AttributeValue n(long value) { return AttributeValue.fromN(Long.toString(value)); }
}

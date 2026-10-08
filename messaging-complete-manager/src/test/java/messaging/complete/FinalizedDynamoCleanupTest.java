package messaging.complete;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class FinalizedDynamoCleanupTest {
    private static final String ID = "a".repeat(32);
    private static final String DECISION = "decision-1";
    private static final Instant NOW = Instant.parse("2026-10-08T00:00:00Z");
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final FinalizedDynamoCleanup cleanup = new FinalizedDynamoCleanup(
            mock(JdbcTemplate.class), db, Clock.fixed(NOW, ZoneOffset.UTC), 25, Duration.ofDays(7));
    private final FinalizedDynamoCleanup.Pending item =
            new FinalizedDynamoCleanup.Pending(ID, DECISION, "PRIMARY");

    @Test
    void waitsForCustomerWebhookHandoffBeforeDeletingAnything() {
        when(db.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
            GetItemRequest request = call.getArgument(0);
            return GetItemResponse.builder().item(request.tableName().equals("ORIGIN")
                    ? origin() : Map.of("status", s("FINALIZED_PUBLISHED"))).build();
        });

        assertFalse(cleanup.cleanup(item));

        verify(db, never()).query(any(QueryRequest.class));
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
        var deletes = org.mockito.ArgumentCaptor.forClass(DeleteItemRequest.class);
        verify(db, times(3)).deleteItem(deletes.capture());
        assertEquals("STEP", deletes.getAllValues().get(0).tableName());
        assertEquals("STEP", deletes.getAllValues().get(1).tableName());
        assertEquals("ORIGIN", deletes.getAllValues().get(2).tableName());
    }

    @Test
    void resumesPartialCleanupAfterOriginWasDeleted() {
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(Map.of("sk", s("RESULT_INBOX#late"))).build());

        assertTrue(cleanup.cleanup(item));

        verify(db).deleteItem(argThat((DeleteItemRequest request) -> request.tableName().equals("STEP")));
        verify(db, never()).deleteItem(argThat((DeleteItemRequest request) -> request.tableName().equals("ORIGIN")));
    }

    private static Map<String, AttributeValue> origin() {
        return Map.of("status", s("PRIMARY_SUCCEEDED"), "primary_decision_id", s(DECISION));
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

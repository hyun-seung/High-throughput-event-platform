package messaging.http.sender;

import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.HttpOutcome;
import messaging.common.delivery.DeliveryIds;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class HttpAttemptRepositoryTest {
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final HttpAttemptRepository repository = new HttpAttemptRepository(db, mapper);
    private final MessageSubmission event = new MessageSubmission("00000000-0000-0000-0000-000000000001", 42,
            "customer-1", "01012345678", MessageCategory.GENERAL, Map.of("message", "hello"), false,
            Instant.parse("2026-10-02T00:00:00Z"));

    @Test
    void originCheckAndStepClaimShareOneTransaction() {
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder()
                .item(MessageOriginCodec.encode(event, mapper)).build());

        var claim = repository.claim(event, "mock-provider", event.receivedAt(),
                event.receivedAt().plusSeconds(30), event.receivedAt().plusSeconds(10800));

        assertEquals(HttpAttemptRepository.State.CLAIMED, claim.state());
        @SuppressWarnings("unchecked")
        var builder = org.mockito.ArgumentCaptor.forClass(Consumer.class);
        verify(db).transactWriteItems(builder.capture());
        var requestBuilder = TransactWriteItemsRequest.builder();
        builder.getValue().accept(requestBuilder);
        var writes = requestBuilder.build().transactItems();
        assertEquals(2, writes.size());
        assertEquals("RECEIVED", writes.get(0).conditionCheck().expressionAttributeValues().get(":received").s());
        assertEquals("PROCESSING", writes.get(1).put().item().get("status").s());
        assertTrue(writes.get(1).put().conditionExpression().contains("attribute_not_exists"));
    }

    @Test
    void duplicateStepReturnsStoredOutcomeWithoutNewClaim() {
        String attemptId = DeliveryIds.attemptId(event.clientMsgId(), "mock-provider", 1, 1);
        var outcome = new HttpOutcome("outcome-1", event.clientMsgId(), attemptId, 1, 1,
                HttpOutcome.Kind.ACCEPTED, event.receivedAt(), event.receivedAt());
        var existing = new HashMap<String, AttributeValue>();
        existing.put("delivery_id", AttributeValue.fromS(event.clientMsgId()));
        existing.put("attempt_id", AttributeValue.fromS(attemptId));
        existing.put("outcome_event", AttributeValue.fromS(mapper.writeValueAsString(outcome)));
        when(db.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(MessageOriginCodec.encode(event, mapper)).build())
                .thenReturn(GetItemResponse.builder().item(existing).build());
        when(db.transactWriteItems(any(Consumer.class))).thenThrow(TransactionCanceledException.builder()
                .cancellationReasons(CancellationReason.builder().code("ConditionalCheckFailed").build()).build());

        var claim = repository.claim(event, "mock-provider", event.receivedAt(),
                event.receivedAt().plusSeconds(30), event.receivedAt().plusSeconds(10800));

        assertEquals(HttpAttemptRepository.State.OBSERVED, claim.state());
        assertEquals(outcome, claim.outcome());
    }
}

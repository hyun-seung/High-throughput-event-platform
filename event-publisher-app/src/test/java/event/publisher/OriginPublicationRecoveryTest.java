package event.publisher;

import event.common.events.EventOriginCodec;
import event.common.events.EventSubmission;
import event.common.events.EventType;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OriginPublicationRecoveryTest {
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final OriginStreamProcessor publisher = mock(OriginStreamProcessor.class);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final OriginPublicationRecovery recovery = new OriginPublicationRecovery(db, publisher, mapper,
            Clock.fixed(Instant.parse("2026-10-02T00:02:00Z"), ZoneOffset.UTC), 10);
    private final EventSubmission event = new EventSubmission("00000000-0000-0000-0000-000000000001", 42,
            "client-1", "01012345678", EventType.GENERAL, Map.of("message", "hello"), false,
            Instant.parse("2026-10-02T00:00:00Z"));

    @Test
    void replaysOnlyAnActiveOriginWithoutStepEvidence() {
        var origin = EventOriginCodec.encode(event, mapper);
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(origin).build());
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());

        recovery.recover(EventOriginCodec.key(event.executionId()));

        verify(publisher).publishIfActive(event);
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void stepEvidenceRemovesRecoveryEntryWithoutReplaying() {
        var origin = EventOriginCodec.encode(event, mapper);
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(origin).build());
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().items(
                Map.of("pk", AttributeValue.fromS("DELIVERY#" + event.executionId()))).build());

        recovery.recover(EventOriginCodec.key(event.executionId()));

        verifyNoInteractions(publisher);
        verify(db).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void finalizedOriginNeverReplays() {
        var origin = new HashMap<>(EventOriginCodec.encode(event, mapper));
        origin.put("completion_event_id", AttributeValue.fromS("done"));
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(origin).build());

        recovery.recover(EventOriginCodec.key(event.executionId()));

        verifyNoInteractions(publisher);
        verify(db, never()).query(any(QueryRequest.class));
    }
}

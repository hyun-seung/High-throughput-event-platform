package messaging.publisher;

import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageTopics;
import messaging.common.messages.MessageCategory;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
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
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OriginPublicationRecoveryTest {
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, MessageSubmission> kafka = mock(KafkaTemplate.class);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final OriginPublicationRecovery recovery = new OriginPublicationRecovery(db, kafka, mapper,
            Clock.fixed(Instant.parse("2026-10-02T00:02:00Z"), ZoneOffset.UTC), 10);
    private final MessageSubmission event = new MessageSubmission("00000000-0000-0000-0000-000000000001", 42,
            "client-1", "01012345678", MessageCategory.GENERAL, Map.of("message", "hello"), false,
            Instant.parse("2026-10-02T00:00:00Z"));

    @Test
    void replaysOnlyAnActiveOriginWithoutStepEvidence() {
        var origin = MessageOriginCodec.encode(event, mapper);
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(origin).build());
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());
        when(kafka.send(MessageTopics.RECEIVED, event.clientMsgId(), event))
                .thenReturn(CompletableFuture.completedFuture(null));

        recovery.recover(MessageOriginCodec.key(event.clientMsgId()));

        verify(kafka).send(MessageTopics.RECEIVED, event.clientMsgId(), event);
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void stepEvidenceRemovesRecoveryEntryWithoutReplaying() {
        var origin = MessageOriginCodec.encode(event, mapper);
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(origin).build());
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().items(
                Map.of("pk", AttributeValue.fromS("DELIVERY#" + event.clientMsgId()))).build());

        recovery.recover(MessageOriginCodec.key(event.clientMsgId()));

        verifyNoInteractions(kafka);
        verify(db).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void finalizedOriginNeverReplays() {
        var origin = new HashMap<>(MessageOriginCodec.encode(event, mapper));
        origin.put("completion_event_id", AttributeValue.fromS("done"));
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(origin).build());

        recovery.recover(MessageOriginCodec.key(event.clientMsgId()));

        verifyNoInteractions(kafka);
        verify(db, never()).query(any(QueryRequest.class));
    }
}

package event.publisher;

import event.common.events.EventOriginCodec;
import event.common.events.EventSubmission;
import event.common.events.EventTopics;
import event.common.events.EventType;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OriginStreamProcessorTest {
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, EventSubmission> kafka = mock(KafkaTemplate.class);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final OriginStreamProcessor processor = new OriginStreamProcessor(db, kafka, mapper);
    private final EventSubmission event = new EventSubmission("00000000-0000-0000-0000-000000000001", 42,
            "client-1", "01012345678", EventType.GENERAL, Map.of("message", "hello"), false,
            Instant.parse("2026-10-02T00:00:00Z"));

    @Test
    void publishesOnlyWhenOriginIsStillActive() {
        var active = EventOriginCodec.encode(event, mapper);
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(active).build());
        when(kafka.send(EventTopics.HTTP_REQUESTED, event.executionId(), event))
                .thenReturn(CompletableFuture.completedFuture(null));

        processor.publishIfActive(event);

        verify(kafka).send(EventTopics.HTTP_REQUESTED, event.executionId(), event);
    }

    @Test
    void skipsOriginAlreadyFinalizedBeforeStreamReplay() {
        var finalized = new HashMap<>(EventOriginCodec.encode(event, mapper));
        finalized.put("completion_event_id", AttributeValue.fromS("finalized-1"));
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(finalized).build());

        processor.publishIfActive(event);

        verifyNoInteractions(kafka);
    }
}

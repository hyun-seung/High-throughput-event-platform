package messaging.result;

import messaging.common.messages.CarrierHttpResult;
import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageResultInboxIndex;
import messaging.common.messages.MessageWebhookBatch;
import messaging.common.messages.MessageWebhookResult;
import messaging.common.messages.PreSendFailure;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MessageResultConsumerTest {
    private static final Instant NOW = Instant.parse("2026-10-07T00:00:00Z");
    private static final String ID = "a".repeat(32);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final PendingResultDispatcher dispatcher = mock(PendingResultDispatcher.class);
    private final MessageResultConsumer consumer = new MessageResultConsumer(new MessageResultRecordCodec(mapper),
            new MessageResultInboxStore(db, mapper, Clock.fixed(NOW, ZoneOffset.UTC)), dispatcher, Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void oneWebhookKafkaRecordBecomesOnePendingItemPerMessage() {
        var results = new ArrayList<MessageWebhookResult>();
        for (int i = 0; i < 100; i++) {
            results.add(new MessageWebhookResult(String.format("%032d", i), "success", null));
        }
        var batch = new MessageWebhookBatch("trace-1", "WEBHOOK", HttpCarrier.KT, NOW, results);

        consumer.receive(record("trace-1", batch));

        var requests = org.mockito.ArgumentCaptor.forClass(PutItemRequest.class);
        verify(db, times(100)).putItem(requests.capture());
        assertEquals(100, requests.getAllValues().stream().map(request -> request.item().get("pk").s()).distinct().count());
        for (var request : requests.getAllValues()) {
            assertEquals("PENDING", request.item().get("status").s());
            assertEquals("WEBHOOK", request.item().get("source").s());
            assertFalse(request.item().containsKey(MessageResultInboxIndex.BUCKET));
            assertEquals(NOW.plusSeconds(7 * 24 * 60 * 60).getEpochSecond(),
                    Long.parseLong(request.item().get("ttl_epoch_seconds").n()));
            assertFalse(request.item().containsKey(MessageResultInboxIndex.DUE));
        }
        verify(dispatcher, times(100)).process(any(MessageResultInboxStore.Item.class), eq(NOW));
    }

    @Test
    void httpAndPreSendInputsAreProcessedWithinTheListener() {
        var failure = new CarrierHttpResult("result-2", ID, "attempt-1", HttpCarrier.KT, 4,
                "HTTP_RESPONSE", CarrierHttpResult.Status.FAILED, 400, "4xx", "41001", 66002,
                "tps", NOW);
        consumer.receive(record(ID, failure));
        consumer.receive(record(ID, PreSendFailure.of(ID, PreSendFailure.Reason.CONTRACT_MISSING, NOW)));
        var order = inOrder(db, dispatcher);
        order.verify(db).putItem(any(PutItemRequest.class));
        order.verify(dispatcher).process(argThat(item -> item.source().equals("HTTP_RESPONSE")), eq(NOW));
        order.verify(db).putItem(any(PutItemRequest.class));
        order.verify(dispatcher).process(argThat(item -> item.source().equals("PRE_SEND")), eq(NOW));
    }

    @Test
    void kafkaHandoffFailurePreventsListenerCompletionAndRetainsCapturedInput() {
        var failure = PreSendFailure.of(ID, PreSendFailure.Reason.CONTRACT_MISSING, NOW);
        doThrow(new IllegalStateException("Kafka unavailable")).when(dispatcher).process(any(), any());
        assertThrows(IllegalStateException.class, () -> consumer.receive(record(ID, failure)));
        verify(db).putItem(any(PutItemRequest.class));
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void persistenceFailurePreventsTheListenerFromCompleting() {
        var failure = PreSendFailure.of(ID, PreSendFailure.Reason.CONTRACT_DISABLED, NOW);
        when(db.putItem(any(PutItemRequest.class))).thenThrow(new IllegalStateException("DynamoDB unavailable"));

        assertThrows(IllegalStateException.class, () -> consumer.receive(record(ID, failure)));
    }

    @Test
    void replayOfAnAlreadyCapturedResultReusesItsExactPayload() {
        var failure = PreSendFailure.of(ID, PreSendFailure.Reason.CONTRACT_MISSING, NOW);
        var captured = new java.util.HashMap<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue>();
        when(db.putItem(any(PutItemRequest.class))).thenAnswer(call -> {
            var request = call.getArgument(0, PutItemRequest.class);
            if (captured.isEmpty()) {
                captured.putAll(request.item());
                return software.amazon.awssdk.services.dynamodb.model.PutItemResponse.builder().build();
            }
            throw ConditionalCheckFailedException.builder().build();
        });
        when(db.getItem(any(software.amazon.awssdk.services.dynamodb.model.GetItemRequest.class)))
                .thenAnswer(call -> GetItemResponse.builder().item(captured).build());

        consumer.receive(record(ID, failure));
        consumer.receive(record(ID, failure));

        verify(db, times(2)).putItem(any(PutItemRequest.class));
        assertEquals("PENDING", captured.get("status").s());
    }

    @Test
    void observationWaitExplicitlyAddsThePendingInputIndex() {
        var store = new MessageResultInboxStore(db, mapper, Clock.fixed(NOW, ZoneOffset.UTC));
        var failure = PreSendFailure.of(ID, PreSendFailure.Reason.CONTRACT_MISSING, NOW);
        var item = store.capture(new MessageResultInput.PreSend(failure)).getFirst();
        store.defer(item, NOW.plusSeconds(5));
        verify(db).updateItem(argThat((UpdateItemRequest request) ->
                request.updateExpression().contains("SET #bucket = :bucket, #due = :due")
                        && request.expressionAttributeValues().get(":bucket").s().equals(MessageResultInboxIndex.bucket(ID))
                        && request.expressionAttributeValues().get(":due").n().equals(Long.toString(NOW.plusSeconds(5).toEpochMilli()))));
    }

    private ConsumerRecord<String, String> record(String key, Object value) {
        return new ConsumerRecord<>("MSG_RESULT", 0, 0L, key, mapper.writeValueAsString(value));
    }
}

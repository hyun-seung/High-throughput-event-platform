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
    private final HttpFailureFollowupService http = mock(HttpFailureFollowupService.class);
    private final MessageResultConsumer consumer = new MessageResultConsumer(new MessageResultRecordCodec(mapper),
            new MessageResultInboxStore(db, mapper, Clock.fixed(NOW, ZoneOffset.UTC)), http);

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
            assertTrue(request.item().containsKey(MessageResultInboxIndex.BUCKET));
            assertEquals(NOW.toEpochMilli(), Long.parseLong(request.item().get(MessageResultInboxIndex.DUE).n()));
        }
        verifyNoInteractions(http);
    }

    @Test
    void httpFailureIsMarkedProcessedOnlyAfterItsFollowupIsFrozen() {
        var failure = new CarrierHttpResult("result-1", ID, "attempt-1", HttpCarrier.KT, 1,
                "HTTP_RESPONSE", CarrierHttpResult.Status.FAILED, 400, "4xx", "41001", 66002,
                "tps", NOW);
        when(http.process(failure)).thenReturn(new HttpFailureFollowupService.Ignored());

        consumer.receive(record(ID, failure));

        verify(db).putItem(any(PutItemRequest.class));
        verify(http).process(failure);
        verify(db).updateItem(argThat((UpdateItemRequest request) ->
                request.updateExpression().contains("REMOVE #bucket, #due")));
    }

    @Test
    void exhaustedHttpFailureAndPreSendFailureRemainPendingForNextStage() {
        var failure = new CarrierHttpResult("result-2", ID, "attempt-1", HttpCarrier.KT, 4,
                "HTTP_RESPONSE", CarrierHttpResult.Status.FAILED, 400, "4xx", "41001", 66002,
                "tps", NOW);
        when(http.process(failure)).thenReturn(new HttpFailureFollowupService.PrimaryFailurePending(40001));

        consumer.receive(record(ID, failure));
        consumer.receive(record(ID, PreSendFailure.of(ID, PreSendFailure.Reason.CONTRACT_MISSING, NOW)));

        verify(db, times(2)).putItem(any(PutItemRequest.class));
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

    private ConsumerRecord<String, String> record(String key, Object value) {
        return new ConsumerRecord<>("MSG_RESULT", 0, 0L, key, mapper.writeValueAsString(value));
    }
}

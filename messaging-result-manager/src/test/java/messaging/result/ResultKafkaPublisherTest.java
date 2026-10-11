package messaging.result;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import messaging.common.messages.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ResultKafkaPublisherTest {
    private static final String ID = "a".repeat(32);
    private static final Instant NOW = Instant.parse("2026-10-11T00:00:00Z");
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> kafka = mock(KafkaTemplate.class);
    private final ResultKafkaPublisher publisher = new ResultKafkaPublisher(db, mapper, kafka, 1000,
            new SimpleMeterRegistry());

    @BeforeEach
    void successfulAcks() {
        when(kafka.send(anyString(), eq(ID), any())).thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void finalResultAndCustomerCommandAreSentDirectlyBeforeCleanupReceipt() {
        var origin = origin(false);
        publisher.publish(origin);
        var order = inOrder(kafka, db);
        order.verify(kafka).send(eq(MessageTopics.MSG_RESULT_FINALIZED), eq(ID), any(FinalizedMessageResult.class));
        order.verify(kafka).send(eq(MessageTopics.WEBHOOK_SEND), eq(ID), any(CustomerWebhookSendCommand.class));
        order.verify(db).updateItem(argThat((UpdateItemRequest request) -> request.tableName().equals("ORIGIN")
                && request.expressionAttributeNames().get("#ack").equals(MessageOriginCodec.FINAL_KAFKA_ACK_DECISION_ID)));
        verify(db, never()).putItem(any(software.amazon.awssdk.services.dynamodb.model.PutItemRequest.class));
    }

    @Test
    void pendingAckHoldsProcessingAndDoesNotAllowCleanup() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var ack = new CompletableFuture<SendResult<String, Object>>();
        when(kafka.send(eq(MessageTopics.MSG_RESULT_FINALIZED), eq(ID), any())).thenAnswer(call -> {
            entered.countDown();
            return ack;
        });
        try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            var processing = executor.submit(() -> publisher.publish(origin(false)));
            try {
                assertTrue(entered.await(1, TimeUnit.SECONDS));
                assertFalse(processing.isDone());
                verify(db, never()).updateItem(any(UpdateItemRequest.class));
                verify(kafka, never()).send(eq(MessageTopics.WEBHOOK_SEND), anyString(), any());
            } finally {
                ack.complete(null);
            }
            processing.get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void failedFinalAckPropagatesAndDoesNotSendWebhookOrReceipt() {
        when(kafka.send(eq(MessageTopics.MSG_RESULT_FINALIZED), eq(ID), any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Kafka unavailable")));
        assertThrows(IllegalStateException.class, () -> publisher.publish(origin(false)));
        verify(kafka, never()).send(eq(MessageTopics.WEBHOOK_SEND), anyString(), any());
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void failedWebhookAckKeepsCleanupBlockedAndRetryUsesSameFinalizedEvent() {
        var origin = origin(false);
        when(kafka.send(eq(MessageTopics.WEBHOOK_SEND), eq(ID), any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Kafka unavailable")),
                        CompletableFuture.completedFuture(null));
        assertThrows(IllegalStateException.class, () -> publisher.publish(origin));
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
        publisher.publish(origin);
        var events = org.mockito.ArgumentCaptor.forClass(Object.class);
        verify(kafka, times(2)).send(eq(MessageTopics.MSG_RESULT_FINALIZED), eq(ID), events.capture());
        assertEquals(events.getAllValues().getFirst(), events.getAllValues().getLast());
        verify(db, times(1)).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void timeoutPropagatesWithoutWritingAckReceipt() {
        when(kafka.send(eq(MessageTopics.MSG_RESULT_FINALIZED), eq(ID), any())).thenReturn(new CompletableFuture<>());
        var bounded = new ResultKafkaPublisher(db, mapper, kafka, 10, new SimpleMeterRegistry());
        var failure = assertThrows(IllegalStateException.class, () -> bounded.publish(origin(false)));
        assertInstanceOf(java.util.concurrent.TimeoutException.class, failure.getCause());
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void interruptionIsPreservedAndDoesNotWriteAckReceipt() {
        when(kafka.send(eq(MessageTopics.MSG_RESULT_FINALIZED), eq(ID), any())).thenReturn(new CompletableFuture<>());
        Thread.currentThread().interrupt();
        try {
            assertThrows(IllegalStateException.class, () -> publisher.publish(origin(false)));
            assertTrue(Thread.currentThread().isInterrupted());
            verify(db, never()).updateItem(any(UpdateItemRequest.class));
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void secondaryRequiredSendsOnlyTcpAndDoesNotPermitFinalCleanup() {
        publisher.publish(origin(true));
        verify(kafka).send(eq(MessageTopics.TCP_SEND), eq(ID), any(SecondarySendCommand.class));
        verify(kafka, never()).send(eq(MessageTopics.MSG_RESULT_FINALIZED), anyString(), any());
        verify(db).updateItem(argThat((UpdateItemRequest request) ->
                !request.expressionAttributeNames().get("#ack").equals(MessageOriginCodec.FINAL_KAFKA_ACK_DECISION_ID)));
    }

    @Test
    void tcpDecisionFinalizesWithTheSecondaryDecisionIdentity() {
        var origin = origin(true);
        var primary = mapper.readValue(origin.get("primary_decision").s(), PrimaryStageDecision.class);
        var submission = MessageOriginCodec.decode(origin, mapper);
        var command = SecondarySendCommand.from(primary, submission);
        var tcp = new TcpSendResult(TcpSendResult.id(ID, command.attemptId()), ID, command.attemptId(), "TCP_RESPONSE",
                TcpSendResult.Status.SUCCESS, null, "RECEIVED", NOW);
        origin.put("secondary_decision", s(mapper.writeValueAsString(SecondaryStageDecision.from(tcp))));
        origin.put("secondary_decision_id", s(tcp.resultId()));
        publisher.publish(origin);
        verify(kafka).send(eq(MessageTopics.MSG_RESULT_FINALIZED), eq(ID), argThat(value ->
                value instanceof FinalizedMessageResult result && result.secondaryDecision().decisionId().equals(tcp.resultId())));
        verify(kafka, never()).send(eq(MessageTopics.TCP_SEND), anyString(), any());
        verify(db).updateItem(argThat((UpdateItemRequest request) ->
                request.expressionAttributeValues().get(":id").s().equals(tcp.resultId())));
    }

    @Test
    void completedReceiptSkipsDuplicateSends() {
        var origin = origin(false);
        origin.put(MessageOriginCodec.FINAL_KAFKA_ACK_DECISION_ID, s("primary-result"));
        publisher.publish(origin);
        verifyNoInteractions(kafka, db);
    }

    private Map<String, AttributeValue> origin(boolean secondary) {
        var submission = new MessageSubmission(ID, 42L, "customer-1", "01012345678",
                MessageCategory.GENERAL, Map.of("text", "first"), secondary ? Map.of("text", "backup") : null, NOW);
        var primary = new PrimaryStageDecision("primary-result", ID,
                secondary ? PrimaryStageDecision.Kind.FAILURE : PrimaryStageDecision.Kind.SUCCESS,
                "WEBHOOK", secondary ? 66999 : null, null, HttpCarrier.SKT, 1, secondary, NOW);
        var origin = new HashMap<>(MessageOriginCodec.encode(submission, mapper));
        origin.put("primary_decision", s(mapper.writeValueAsString(primary)));
        origin.put("primary_decision_id", s(primary.decisionId()));
        origin.put("status", s(secondary ? "SECONDARY_PENDING" : "PRIMARY_SUCCEEDED"));
        return origin;
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}

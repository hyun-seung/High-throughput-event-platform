package messaging.result;

import messaging.common.messages.CustomerWebhookSendCommand;
import messaging.common.messages.FinalizedMessageResult;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageResultInboxIndex;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageTopics;
import messaging.common.messages.PrimaryStageDecision;
import messaging.common.messages.SecondarySendCommand;
import messaging.common.messages.SecondaryStageDecision;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PrimaryDecisionOutboxDispatcherTest {
    private static final Instant NOW = Instant.parse("2026-10-07T00:01:00Z");
    private static final String ID = "a".repeat(32);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> kafka = mock(KafkaTemplate.class);
    private final PrimaryDecisionOutboxDispatcher dispatcher = new PrimaryDecisionOutboxDispatcher(db, mapper,
            kafka, Clock.fixed(NOW, ZoneOffset.UTC), 100);

    @Test
    void successPublishesFinalizedAndCustomerWebhookWithSeparateDurableCheckpoints() throws Exception {
        var decision = decision(PrimaryStageDecision.Kind.SUCCESS, false);
        var submission = submission(false);
        var item = outbox(decision, submission);
        reads(item);
        when(kafka.send(eq(MessageTopics.MSG_RESULT_FINALIZED), eq(ID), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(kafka.send(eq(MessageTopics.WEBHOOK_SEND), eq(ID), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        dispatcher.dispatch(key(item), NOW.toEpochMilli());

        verify(kafka).send(MessageTopics.MSG_RESULT_FINALIZED, ID,
                new FinalizedMessageResult(decision, submission));
        verify(kafka).send(MessageTopics.WEBHOOK_SEND, ID,
                CustomerWebhookSendCommand.from(new FinalizedMessageResult(decision, submission)));
        var updates = org.mockito.ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(db, times(2)).updateItem(updates.capture());
        assertTrue(updates.getAllValues().get(0).updateExpression().contains("finalized_published_at_ms"));
        assertFalse(updates.getAllValues().get(0).updateExpression().contains("REMOVE #bucket"));
        assertTrue(updates.getAllValues().get(1).updateExpression().contains("REMOVE #bucket, #due"));
        var order = inOrder(kafka, db);
        order.verify(kafka).send(MessageTopics.MSG_RESULT_FINALIZED, ID,
                new FinalizedMessageResult(decision, submission));
        order.verify(db).updateItem(argThat((UpdateItemRequest request) ->
                request.updateExpression().contains("finalized_published_at_ms")));
        order.verify(kafka).send(MessageTopics.WEBHOOK_SEND, ID,
                CustomerWebhookSendCommand.from(new FinalizedMessageResult(decision, submission)));
        order.verify(db).updateItem(argThat((UpdateItemRequest request) ->
                request.updateExpression().contains("REMOVE #bucket, #due")));
    }

    @Test
    void restartAfterFinalizedPublicationSendsOnlyCustomerWebhook() throws Exception {
        var decision = decision(PrimaryStageDecision.Kind.FAILURE, false);
        var submission = submission(false);
        var item = outbox(decision, submission);
        item.put("status", AttributeValue.fromS("FINALIZED_PUBLISHED"));
        item.put("finalized_published_at_ms", AttributeValue.fromN(Long.toString(NOW.toEpochMilli())));
        reads(item);
        when(kafka.send(eq(MessageTopics.WEBHOOK_SEND), eq(ID), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        dispatcher.dispatch(key(item), NOW.toEpochMilli());

        verify(kafka, never()).send(eq(MessageTopics.MSG_RESULT_FINALIZED), anyString(), any());
        verify(kafka).send(MessageTopics.WEBHOOK_SEND, ID,
                CustomerWebhookSendCommand.from(new FinalizedMessageResult(decision, submission)));
        verify(db).updateItem(argThat((UpdateItemRequest request) ->
                request.conditionExpression().contains("finalized_published_at_ms")));
    }

    @Test
    void uncertainCustomerWebhookAcknowledgementKeepsTheIntermediateStateDue() {
        var item = outbox(decision(PrimaryStageDecision.Kind.SUCCESS, false), submission(false));
        reads(item);
        when(kafka.send(eq(MessageTopics.MSG_RESULT_FINALIZED), eq(ID), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(kafka.send(eq(MessageTopics.WEBHOOK_SEND), eq(ID), any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Kafka unavailable")));

        assertThrows(ExecutionException.class, () -> dispatcher.dispatch(key(item), NOW.toEpochMilli()));

        verify(db).updateItem(argThat((UpdateItemRequest request) ->
                request.updateExpression().contains("finalized_published_at_ms")));
        verify(db, times(1)).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void secondaryFailurePublishesTheFrozenTcpCommand() throws Exception {
        var decision = decision(PrimaryStageDecision.Kind.FAILURE, true);
        var submission = submission(true);
        var item = outbox(decision, submission);
        reads(item);
        when(kafka.send(eq(MessageTopics.TCP_SEND), eq(ID), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        dispatcher.dispatch(key(item), NOW.toEpochMilli());

        verify(kafka).send(MessageTopics.TCP_SEND, ID,
                SecondarySendCommand.from(decision, submission));
        verify(kafka, never()).send(eq(MessageTopics.WEBHOOK_SEND), anyString(), any());
        verify(db).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void tcpFinalResultPublishesHistoryAndCustomerWebhookWithoutAnotherTcpCommand() throws Exception {
        var primary = decision(PrimaryStageDecision.Kind.FAILURE, true);
        var submission = submission(true);
        var secondary = new SecondaryStageDecision("tcp-result", ID, "tcp-attempt",
                SecondaryStageDecision.Kind.SUCCESS, null, "RECEIVED", NOW.plusSeconds(1));
        var finalized = new FinalizedMessageResult(primary, secondary, submission);
        var item = outbox(primary, submission);
        item.put("sk", AttributeValue.fromS("SECONDARY_DECISION#" + secondary.decisionId()));
        item.put("result_payload", AttributeValue.fromS(mapper.writeValueAsString(finalized)));
        reads(item);
        when(kafka.send(eq(MessageTopics.MSG_RESULT_FINALIZED), eq(ID), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(kafka.send(eq(MessageTopics.WEBHOOK_SEND), eq(ID), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        dispatcher.dispatch(key(item), NOW.toEpochMilli());

        verify(kafka).send(MessageTopics.MSG_RESULT_FINALIZED, ID, finalized);
        verify(kafka).send(MessageTopics.WEBHOOK_SEND, ID, CustomerWebhookSendCommand.from(finalized));
        verify(kafka, never()).send(eq(MessageTopics.TCP_SEND), anyString(), any());
    }

    @Test
    void uncertainKafkaAcknowledgementLeavesTheSameOutboxDue() {
        var item = outbox(decision(PrimaryStageDecision.Kind.SUCCESS, false), submission(false));
        reads(item);
        when(kafka.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Kafka unavailable")));

        assertThrows(ExecutionException.class, () -> dispatcher.dispatch(key(item), NOW.toEpochMilli()));
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    private PrimaryStageDecision decision(PrimaryStageDecision.Kind kind, boolean secondary) {
        return new PrimaryStageDecision("result-1", ID, kind, "WEBHOOK",
                kind == PrimaryStageDecision.Kind.FAILURE ? 66003 : null,
                null, messaging.common.messages.HttpCarrier.SKT, 1, secondary, NOW);
    }

    private MessageSubmission submission(boolean secondary) {
        return new MessageSubmission(ID, 42L, "customer-id", "01012345678", MessageCategory.GENERAL,
                Map.of("text", "hello"), secondary ? Map.of("text", "backup") : null, NOW.minusSeconds(60));
    }

    private Map<String, AttributeValue> outbox(PrimaryStageDecision decision, MessageSubmission submission) {
        var item = new HashMap<String, AttributeValue>();
        item.put("pk", AttributeValue.fromS("DELIVERY#" + ID));
        item.put("sk", AttributeValue.fromS("PRIMARY_DECISION#" + decision.decisionId()));
        item.put("result_payload", AttributeValue.fromS(mapper.writeValueAsString(decision)));
        item.put("submission_payload", AttributeValue.fromS(mapper.writeValueAsString(submission)));
        item.put("status", AttributeValue.fromS("PENDING"));
        MessageResultInboxIndex.add(item, ID, NOW.toEpochMilli());
        return item;
    }

    private Map<String, AttributeValue> key(Map<String, AttributeValue> item) {
        return Map.of("pk", item.get("pk"), "sk", item.get("sk"));
    }

    private void reads(Map<String, AttributeValue> item) {
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());
    }
}

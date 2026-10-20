package messaging.result;

import messaging.common.messages.CarrierHttpResult;
import messaging.common.messages.HttpCarrier;
import messaging.common.messages.HttpProviderRequest;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageWebhookResult;
import messaging.common.messages.PreSendDispatch;
import messaging.common.messages.PreSendFailure;
import messaging.common.messages.PrimaryStageDecision;
import messaging.common.messages.PrimaryExpiryIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PrimaryStageDecisionStoreTest {
    private static final Instant NOW = Instant.parse("2026-10-07T00:01:00Z");
    private static final String ID = "a".repeat(32);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final ResultKafkaPublisher publisher = mock(ResultKafkaPublisher.class);
    private final PrimaryStageDecisionStore store = new PrimaryStageDecisionStore(db, mapper,
            Clock.fixed(NOW, ZoneOffset.UTC), publisher);
    private final MessageSubmission admission = new MessageSubmission(ID, 42L, "customer-id", "01012345678",
            MessageCategory.GENERAL, Map.of("text", "hello"), null, NOW.minusSeconds(60));
    private final HttpSendCommand command = new HttpSendCommand(HttpSendCommand.attemptId(ID, HttpCarrier.SKT),
            HttpCarrier.SKT, 1, NOW.plusSeconds(3600), new HttpProviderRequest(ID, 42L, "GENERAL",
            "01012345678", admission.payload(), admission.receivedAt()));

    @Test
    void successWebhookClosesPrimaryBeforeDirectKafkaHandoff() {
        var origin = origin(false, new PreSendDispatch(command, null));
        var item = webhook("success", null);

        assertEquals(PrimaryStageDecisionStore.Outcome.STORED,
                store.fromWebhook(item, null, command, null, origin));

        var writes = writes();
        assertEquals("PRIMARY_SUCCEEDED", writes.expressionAttributeValues().get(":next").s());
        var decision = mapper.readValue(writes.expressionAttributeValues().get(":decision").s(),
                PrimaryStageDecision.class);
        assertEquals(PrimaryStageDecision.Kind.SUCCESS, decision.kind());
        assertFalse(decision.secondaryRequired());
        assertEquals(item.resultId(), decision.decisionId());
        assertTrue(writes.conditionExpression().contains("attribute_not_exists(primary_decision_id)"));
        assertTrue(writes.conditionExpression().contains("attribute_not_exists(completion_event_id)"));
        assertTrue(writes.conditionExpression().contains("#status = :received"));
        assertTrue(writes.conditionExpression().contains("pre_send_dispatch = :dispatch"));
        assertTrue(writes.conditionExpression().contains("attribute_not_exists(#currentDecision)"));
        assertEquals(mapper.writeValueAsString(new PreSendDispatch(command, null)),
                writes.expressionAttributeValues().get(":dispatch").s());
        verify(db, never()).getItem(any(GetItemRequest.class));
    }

    @Test
    void preSendFailureWithSecondaryPayloadStopsPrimaryAndWaitsForTcp() {
        var failure = PreSendFailure.of(ID, PreSendFailure.Reason.CONTRACT_MISSING, NOW);
        reads(origin(true, new PreSendDispatch(null, failure)));
        var item = new MessageResultInboxStore.Item(ID, failure.resultId(), "PRE_SEND",
                mapper.writeValueAsString(failure), NOW);

        assertEquals(PrimaryStageDecisionStore.Outcome.STORED, store.fromPreSend(item));

        var writes = writes();
        assertEquals("SECONDARY_PENDING", writes.expressionAttributeValues().get(":next").s());
        var decision = mapper.readValue(writes.expressionAttributeValues().get(":decision").s(),
                PrimaryStageDecision.class);
        assertTrue(decision.secondaryRequired());
        assertEquals("CONTRACT_MISSING", decision.reason());
        assertNull(decision.errorCode());
    }

    @Test
    void exhaustedHttpFailureWithoutSecondaryClosesPrimaryAsFailure() {
        reads(origin(false, new PreSendDispatch(command, null)));
        var http = new CarrierHttpResult(CarrierHttpResult.id(command), ID, command.attemptId(),
                HttpCarrier.SKT, 1, "HTTP_RESPONSE", CarrierHttpResult.Status.FAILED,
                400, "4xx", "41001", 66002, "tps", NOW);
        var item = new MessageResultInboxStore.Item(ID, http.resultId(), http.source(),
                mapper.writeValueAsString(http), NOW);

        assertEquals(PrimaryStageDecisionStore.Outcome.STORED,
                store.fromHttp(item, new HttpFailureFollowupService.PrimaryFailurePending(40001, null, command)));

        var writes = writes();
        assertEquals("PRIMARY_FAILED", writes.expressionAttributeValues().get(":next").s());
        var decision = mapper.readValue(writes.expressionAttributeValues().get(":decision").s(),
                PrimaryStageDecision.class);
        assertEquals(40001, decision.errorCode());
    }

    @Test
    void alreadyClosedOriginCannotCreateAnotherPrimaryDecision() {
        var closed = origin(false, new PreSendDispatch(command, null));
        closed.put("status", AttributeValue.fromS("PRIMARY_SUCCEEDED"));
        reads(closed);

        assertEquals(PrimaryStageDecisionStore.Outcome.ALREADY_CLOSED,
                store.fromWebhook(webhook("success", null), null, command, null, closed));
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void elapsedDeadlineFreezesExpiryWhenNoEarlierResultIsPending() {
        var expired = overdueOrigin();
        reads(expired);

        assertEquals(PrimaryStageDecisionStore.Outcome.STORED, store.fromExpiry(ID));

        var writes = writes();
        assertEquals("PRIMARY_EXPIRED", writes.expressionAttributeValues().get(":next").s());
        assertFalse(writes.updateExpression().contains("REMOVE"), "Expiry remains retryable until the Kafka ack");
        var decision = mapper.readValue(writes.expressionAttributeValues().get(":decision").s(),
                PrimaryStageDecision.class);
        assertEquals("PRIMARY_EXPIRED", decision.reason());
        assertEquals(PrimaryStageDecision.Kind.FAILURE, decision.kind());
    }

    @Test
    void resultCapturedBeforeTheDeadlineGetsDecidedBeforeExpiry() {
        var expired = overdueOrigin();
        reads(expired);
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().items(Map.of(
                "status", AttributeValue.fromS("PENDING"),
                "received_at", AttributeValue.fromS(NOW.minus(Duration.ofHours(3)).toString()))).build());

        assertEquals(PrimaryStageDecisionStore.Outcome.WAITING, store.fromExpiry(ID));
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void expiredMessageWithoutPreSendDispatchStillClosesAndKeepsSecondaryHandoff() {
        var expired = overdueOrigin();
        expired.remove("pre_send_dispatch");
        expired.put("secondary_send_payload", AttributeValue.fromS("{\"text\":\"backup\"}"));
        reads(expired);

        assertEquals(PrimaryStageDecisionStore.Outcome.STORED, store.fromExpiry(ID));

        var writes = writes();
        assertEquals("SECONDARY_PENDING", writes.expressionAttributeValues().get(":next").s());
        assertTrue(writes.conditionExpression().contains("attribute_not_exists(pre_send_dispatch)"));
        var decision = mapper.readValue(writes.expressionAttributeValues().get(":decision").s(),
                PrimaryStageDecision.class);
        assertEquals("PRIMARY_EXPIRY", decision.source());
        assertTrue(decision.secondaryRequired());
    }

    private Map<String, AttributeValue> overdueOrigin() {
        var receivedAt = NOW.minus(Duration.ofHours(3)).minusSeconds(1);
        var overdue = new MessageSubmission(ID, 42L, "customer-id", "01012345678",
                MessageCategory.GENERAL, Map.of("text", "hello"), null, receivedAt);
        var origin = new HashMap<>(MessageOriginCodec.encode(overdue, mapper));
        var first = new HttpSendCommand(HttpSendCommand.attemptId(ID, HttpCarrier.SKT), HttpCarrier.SKT,
                1, receivedAt.plus(PrimaryExpiryIndex.TTL), new HttpProviderRequest(ID, 42L, "GENERAL",
                "01012345678", overdue.payload(), receivedAt));
        origin.put("pre_send_dispatch", AttributeValue.fromS(
                mapper.writeValueAsString(new PreSendDispatch(first, null))));
        return origin;
    }

    private MessageResultInboxStore.Item webhook(String status, Integer code) {
        var result = new MessageWebhookResult(ID, status,
                code == null ? null : new MessageWebhookResult.Error(code, "failure"));
        var wrapper = new MessageResultInboxStore.WebhookItem("trace-1", HttpCarrier.SKT, NOW, result);
        return new MessageResultInboxStore.Item(ID, MessageResultInboxStore.webhookResultId("trace-1", ID),
                "WEBHOOK", mapper.writeValueAsString(wrapper), NOW);
    }

    private Map<String, AttributeValue> origin(boolean secondary, PreSendDispatch dispatch) {
        var value = new HashMap<>(MessageOriginCodec.encode(admission, mapper));
        if (secondary) value.put("secondary_send_payload", AttributeValue.fromS("{\"text\":\"backup\"}"));
        value.put("pre_send_dispatch", AttributeValue.fromS(mapper.writeValueAsString(dispatch)));
        return value;
    }

    private void reads(Map<String, AttributeValue> origin) {
        when(db.getItem(any(GetItemRequest.class))).thenAnswer(call -> GetItemResponse.builder().item(
                call.getArgument(0, GetItemRequest.class).tableName().equals(
                        messaging.common.dynamodb.DynamoDbTableNames.ORIGIN) ? origin : Map.of()).build());
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());
    }

    private UpdateItemRequest writes() {
        var capture = org.mockito.ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(db).updateItem(capture.capture());
        var order = inOrder(db, publisher);
        order.verify(db).updateItem(any(UpdateItemRequest.class));
        order.verify(publisher).publish(anyMap());
        verify(db, never()).putItem(any(software.amazon.awssdk.services.dynamodb.model.PutItemRequest.class));
        return capture.getValue();
    }

    @Test
    void failedKafkaHandoffReusesFrozenDecisionWithoutWritingAnotherDecision() {
        var origin = origin(false, new PreSendDispatch(command, null));
        reads(origin);
        when(db.updateItem(any(UpdateItemRequest.class))).thenAnswer(call -> {
            var values = call.getArgument(0, UpdateItemRequest.class).expressionAttributeValues();
            origin.put("status", values.get(":next"));
            origin.put("primary_decision_id", values.get(":decisionId"));
            origin.put("primary_decision", values.get(":decision"));
            return software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse.builder().build();
        });
        doThrow(new IllegalStateException("Kafka unavailable")).doNothing().when(publisher).publish(anyMap());
        var item = webhook("success", null);
        assertThrows(IllegalStateException.class, () -> store.fromWebhook(item, null, command, null, origin));
        String frozen = origin.get("primary_decision").s();
        assertEquals(PrimaryStageDecisionStore.Outcome.ALREADY_CLOSED,
                store.fromWebhook(item, null, command, null, origin));
        verify(db, times(1)).updateItem(any(UpdateItemRequest.class));
        verify(publisher, times(2)).publish(argThat(value -> frozen.equals(value.get("primary_decision").s())));
    }

    @Test
    void lostWriteResponseResumesCommittedBusinessDecision() {
        var origin = origin(false, new PreSendDispatch(command, null));
        var snapshot = Map.copyOf(origin);
        reads(origin);
        when(db.updateItem(any(UpdateItemRequest.class))).thenAnswer(call -> {
            var values = call.getArgument(0, UpdateItemRequest.class).expressionAttributeValues();
            origin.put("status", values.get(":next"));
            origin.put("primary_decision_id", values.get(":decisionId"));
            origin.put("primary_decision", values.get(":decision"));
            throw new IllegalStateException("response lost after commit");
        });
        assertEquals(PrimaryStageDecisionStore.Outcome.STORED,
                store.fromWebhook(webhook("success", null), null, command, null, snapshot));
        verify(publisher).publish(origin);
        verify(db).getItem(argThat((GetItemRequest request) -> request.consistentRead()
                && request.key().equals(MessageOriginCodec.key(ID))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"WEBHOOK", "PRIMARY_EXPIRY"})
    void staleSnapshotResumesTheWinningDecisionInsteadOfPublishingTheProposedResult(String source) {
        var snapshot = origin(false, new PreSendDispatch(command, null));
        var latest = new HashMap<>(snapshot);
        var winner = new PrimaryStageDecision("winner", ID, PrimaryStageDecision.Kind.FAILURE, source,
                "WEBHOOK".equals(source) ? 66003 : null,
                "PRIMARY_EXPIRY".equals(source) ? "PRIMARY_EXPIRED" : null,
                command.carrier(), command.invocation(), false, NOW.minusSeconds(10));
        latest.put("status", AttributeValue.fromS("PRIMARY_EXPIRY".equals(source) ? "PRIMARY_EXPIRED" : "PRIMARY_FAILED"));
        latest.put("primary_decision_id", AttributeValue.fromS(winner.decisionId()));
        latest.put("primary_decision", AttributeValue.fromS(mapper.writeValueAsString(winner)));
        reads(latest);
        when(db.updateItem(any(UpdateItemRequest.class)))
                .thenThrow(ConditionalCheckFailedException.builder().message("Another decision won").build());

        assertEquals(PrimaryStageDecisionStore.Outcome.ALREADY_CLOSED,
                store.fromWebhook(webhook("success", null), null, command, null, snapshot));

        verify(db, times(1)).updateItem(any(UpdateItemRequest.class));
        verify(db).getItem(argThat((GetItemRequest request) -> request.consistentRead()));
        verify(publisher).publish(latest);
        verifyNoMoreInteractions(publisher);
    }

    @Test
    void changedInvocationRemainsRetryableAndCannotPublishAStaleSuccess() {
        var snapshot = origin(false, new PreSendDispatch(command, null));
        var latest = new HashMap<>(snapshot);
        var retry = new HttpSendCommand(command.attemptId(), command.carrier(), 2,
                command.deadlineAt(), command.request());
        latest.put(messaging.common.messages.FollowupHttpCommand.CURRENT_DECISION, AttributeValue.fromS("retry"));
        latest.put(messaging.common.messages.FollowupHttpCommand.CURRENT_COMMAND,
                AttributeValue.fromS(mapper.writeValueAsString(retry)));
        reads(latest);
        var conflict = ConditionalCheckFailedException.builder().message("Invocation advanced").build();
        when(db.updateItem(any(UpdateItemRequest.class))).thenThrow(conflict);

        assertSame(conflict, assertThrows(ConditionalCheckFailedException.class,
                () -> store.fromWebhook(webhook("success", null), null, command, null, snapshot)));

        verify(db).getItem(argThat((GetItemRequest request) -> request.consistentRead()));
        verifyNoInteractions(publisher);
    }

    @Test
    void ambiguousWriteWithoutAStoredDecisionRemainsRetryable() {
        var snapshot = origin(false, new PreSendDispatch(command, null));
        reads(snapshot);
        var unavailable = new IllegalStateException("Write response unavailable");
        when(db.updateItem(any(UpdateItemRequest.class))).thenThrow(unavailable);

        assertSame(unavailable, assertThrows(IllegalStateException.class,
                () -> store.fromWebhook(webhook("success", null), null, command, null, snapshot)));

        verify(db).getItem(argThat((GetItemRequest request) -> request.consistentRead()));
        verifyNoInteractions(publisher);
    }

    @Test
    void removedOriginCannotBeRecreatedFromAStaleSnapshot() {
        var snapshot = origin(false, new PreSendDispatch(command, null));
        reads(Map.of());
        when(db.updateItem(any(UpdateItemRequest.class)))
                .thenThrow(ConditionalCheckFailedException.builder().message("Already cleaned up").build());

        assertEquals(PrimaryStageDecisionStore.Outcome.ALREADY_CLOSED,
                store.fromWebhook(webhook("success", null), null, command, null, snapshot));

        verify(db).getItem(argThat((GetItemRequest request) -> request.consistentRead()));
        verifyNoInteractions(publisher);
    }

    @Test
    void rejectsAnotherMessagesOriginBeforeWritingOrPublishing() {
        var snapshot = origin(false, new PreSendDispatch(command, null));
        snapshot.put("delivery_id", AttributeValue.fromS("b".repeat(32)));

        assertThrows(IllegalArgumentException.class,
                () -> store.fromWebhook(webhook("success", null), null, command, null, snapshot));

        verifyNoInteractions(db, publisher);
    }
}

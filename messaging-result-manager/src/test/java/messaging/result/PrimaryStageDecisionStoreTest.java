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
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PrimaryStageDecisionStoreTest {
    private static final Instant NOW = Instant.parse("2026-10-07T00:01:00Z");
    private static final String ID = "a".repeat(32);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final PrimaryStageDecisionStore store = new PrimaryStageDecisionStore(db, mapper,
            Clock.fixed(NOW, ZoneOffset.UTC));
    private final MessageSubmission admission = new MessageSubmission(ID, 42L, "customer-id", "01012345678",
            MessageCategory.GENERAL, Map.of("text", "hello"), null, NOW.minusSeconds(60));
    private final HttpSendCommand command = new HttpSendCommand(HttpSendCommand.attemptId(ID, HttpCarrier.SKT),
            HttpCarrier.SKT, 1, NOW.plusSeconds(3600), new HttpProviderRequest(ID, 42L, "GENERAL",
            "01012345678", admission.payload(), admission.receivedAt()));

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void successWebhookClosesPrimaryAndStoresFinalHandoffAtomically() {
        reads(origin(false, new PreSendDispatch(command, null)));
        var item = webhook("success", null);

        assertEquals(PrimaryStageDecisionStore.Outcome.STORED,
                store.fromWebhook(item, null, command, null));

        var writes = writes();
        assertEquals("PRIMARY_SUCCEEDED", writes.get(0).update().expressionAttributeValues().get(":next").s());
        var decision = mapper.readValue(writes.get(1).put().item().get("result_payload").s(),
                PrimaryStageDecision.class);
        assertEquals(PrimaryStageDecision.Kind.SUCCESS, decision.kind());
        assertFalse(decision.secondaryRequired());
        assertEquals(item.resultId(), decision.decisionId());
        assertTrue(writes.get(0).update().conditionExpression().contains("attribute_not_exists(primary_decision_id)"));
    }

    @Test
    void preSendFailureWithSecondaryPayloadStopsPrimaryAndWaitsForTcp() {
        var failure = PreSendFailure.of(ID, PreSendFailure.Reason.CONTRACT_MISSING, NOW);
        reads(origin(true, new PreSendDispatch(null, failure)));
        var item = new MessageResultInboxStore.Item(ID, failure.resultId(), "PRE_SEND",
                mapper.writeValueAsString(failure), NOW);

        assertEquals(PrimaryStageDecisionStore.Outcome.STORED, store.fromPreSend(item));

        var writes = writes();
        assertEquals("SECONDARY_PENDING", writes.get(0).update().expressionAttributeValues().get(":next").s());
        var decision = mapper.readValue(writes.get(1).put().item().get("result_payload").s(),
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
        assertEquals("PRIMARY_FAILED", writes.get(0).update().expressionAttributeValues().get(":next").s());
        var decision = mapper.readValue(writes.get(1).put().item().get("result_payload").s(),
                PrimaryStageDecision.class);
        assertEquals(40001, decision.errorCode());
    }

    @Test
    void alreadyClosedOriginCannotCreateAnotherPrimaryDecision() {
        var closed = origin(false, new PreSendDispatch(command, null));
        closed.put("status", AttributeValue.fromS("PRIMARY_SUCCEEDED"));
        reads(closed);

        assertEquals(PrimaryStageDecisionStore.Outcome.ALREADY_CLOSED,
                store.fromWebhook(webhook("success", null), null, command, null));
        verify(db, never()).transactWriteItems(any(Consumer.class));
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
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private java.util.List<software.amazon.awssdk.services.dynamodb.model.TransactWriteItem> writes() {
        var capture = org.mockito.ArgumentCaptor.forClass(Consumer.class);
        verify(db).transactWriteItems(capture.capture());
        var builder = TransactWriteItemsRequest.builder();
        ((Consumer<TransactWriteItemsRequest.Builder>) capture.getValue()).accept(builder);
        return builder.build().transactItems();
    }
}

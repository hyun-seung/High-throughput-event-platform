package messaging.result;

import messaging.common.messages.CarrierHttpResult;
import messaging.common.messages.FollowupHttpCommand;
import messaging.common.messages.HttpCarrier;
import messaging.common.messages.HttpProviderRequest;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageWebhookResult;
import messaging.common.messages.PreSendDispatch;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class WebhookPrimaryDecisionServiceTest {
    private static final Instant NOW = Instant.parse("2026-10-07T00:01:00Z");
    private static final String ID = "a".repeat(32);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final FollowupHttpCommandStore commands = mock(FollowupHttpCommandStore.class);
    private final WebhookPrimaryDecisionService service = new WebhookPrimaryDecisionService(db, mapper, commands,
            Clock.fixed(NOW, ZoneOffset.UTC));
    private final MessageSubmission admission = new MessageSubmission(ID, 42L, "customer-id", "01012345678",
            MessageCategory.GENERAL, Map.of("text", "hello"), null, NOW.minusSeconds(60));
    private final HttpSendCommand command = new HttpSendCommand(HttpSendCommand.attemptId(ID, HttpCarrier.SKT),
            HttpCarrier.SKT, 1, NOW.plusSeconds(3600), new HttpProviderRequest(ID, 42L, "GENERAL",
            "01012345678", admission.payload(), admission.receivedAt()));

    @Test
    void tpsWebhookAfterHttpAcceptanceFreezesTheSecondInvocation() {
        reads(origin(), observed(CarrierHttpResult.Status.ACCEPTED));
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(observed(CarrierHttpResult.Status.ACCEPTED)).build());
        when(commands.freeze(eq(ID), isNull(), any(FollowupHttpCommand.class)))
                .thenAnswer(call -> call.getArgument(2));

        var outcome = assertInstanceOf(WebhookPrimaryDecisionService.FollowupStored.class,
                service.process(webhook(HttpCarrier.SKT, "fail", 66002)));

        assertEquals(HttpCarrier.SKT, outcome.value().command().carrier());
        assertEquals(2, outcome.value().command().invocation());
        assertEquals(NOW.plusSeconds(60), outcome.value().notBefore());
        assertEquals(ID, outcome.value().command().request().clientMsgId());
    }

    @Test
    void carrierMismatchWebhookFreezesTheNextUntriedCarrier() {
        reads(origin(), observed(CarrierHttpResult.Status.ACCEPTED));
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(observed(CarrierHttpResult.Status.ACCEPTED)).build());
        when(commands.freeze(eq(ID), isNull(), any(FollowupHttpCommand.class)))
                .thenAnswer(call -> call.getArgument(2));

        var outcome = assertInstanceOf(WebhookPrimaryDecisionService.FollowupStored.class,
                service.process(webhook(HttpCarrier.SKT, "fail", 66001)));

        assertEquals(HttpCarrier.KT, outcome.value().command().carrier());
        assertEquals(1, outcome.value().command().invocation());
        assertEquals(NOW, outcome.value().notBefore());
    }

    @Test
    void aWebhookBeforeTheHttpObservationRemainsPending() {
        reads(origin(), Map.of());
        assertInstanceOf(WebhookPrimaryDecisionService.AwaitingHttp.class,
                service.process(webhook(HttpCarrier.SKT, "success", null)));
        verifyNoInteractions(commands);
    }

    @Test
    void successRemainsPendingUntilFinalResultHandoffExists() {
        reads(origin(), observed(CarrierHttpResult.Status.ACCEPTED));
        assertInstanceOf(WebhookPrimaryDecisionService.SuccessPending.class,
                service.process(webhook(HttpCarrier.SKT, "success", null)));
        verifyNoInteractions(commands);
    }

    @Test
    void oldCarrierWebhookCannotAdvanceTheCurrentInvocation() {
        reads(origin(), observed(CarrierHttpResult.Status.ACCEPTED));
        assertInstanceOf(WebhookPrimaryDecisionService.Ignored.class,
                service.process(webhook(HttpCarrier.KT, "fail", 66001)));
        verifyNoInteractions(commands);
    }

    @Test
    void replayOfAWebhookThatAlreadyScheduledTheSameCarrierRetryIsIgnored() {
        var item = webhook(HttpCarrier.SKT, "fail", 66002);
        var retry = new HttpSendCommand(command.attemptId(), command.carrier(), 2,
                command.deadlineAt(), command.request());
        var origin = origin();
        origin.put(FollowupHttpCommand.CURRENT_DECISION, AttributeValue.fromS(item.resultId()));
        origin.put(FollowupHttpCommand.CURRENT_COMMAND,
                AttributeValue.fromS(mapper.writeValueAsString(retry)));
        reads(origin, Map.of());

        assertInstanceOf(WebhookPrimaryDecisionService.Ignored.class, service.process(item));
        verifyNoInteractions(commands);
    }

    @Test
    void replayOfAnOlderWebhookDecisionIsIgnoredAfterFurtherRetries() {
        var item = webhook(HttpCarrier.SKT, "fail", 66002);
        var retry = new HttpSendCommand(command.attemptId(), command.carrier(), 3,
                command.deadlineAt(), command.request());
        var origin = origin();
        origin.put(FollowupHttpCommand.CURRENT_DECISION, AttributeValue.fromS("later-result"));
        origin.put(FollowupHttpCommand.CURRENT_COMMAND,
                AttributeValue.fromS(mapper.writeValueAsString(retry)));
        reads(origin, Map.of());
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(Map.of("decision_id", AttributeValue.fromS(item.resultId()))).build());

        assertInstanceOf(WebhookPrimaryDecisionService.Ignored.class, service.process(item));
        verifyNoInteractions(commands);
    }

    @Test
    void anOrdinaryFailureIsLeftForSecondaryOrFinalHandling() {
        reads(origin(), observed(CarrierHttpResult.Status.ACCEPTED));
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(observed(CarrierHttpResult.Status.ACCEPTED)).build());

        var pending = assertInstanceOf(WebhookPrimaryDecisionService.PrimaryFailurePending.class,
                service.process(webhook(HttpCarrier.SKT, "fail", 66003)));
        assertEquals(66003, pending.errorCode());
        verifyNoInteractions(commands);
    }

    private MessageResultInboxStore.Item webhook(HttpCarrier carrier, String status, Integer errorCode) {
        var result = new MessageWebhookResult(ID, status,
                errorCode == null ? null : new MessageWebhookResult.Error(errorCode, "failure"));
        var wrapper = new MessageResultInboxStore.WebhookItem("trace-1", carrier, NOW, result);
        return new MessageResultInboxStore.Item(ID, MessageResultInboxStore.webhookResultId("trace-1", ID),
                "WEBHOOK", mapper.writeValueAsString(wrapper), NOW);
    }

    private Map<String, AttributeValue> origin() {
        var value = new HashMap<>(MessageOriginCodec.encode(admission, mapper));
        value.put("pre_send_dispatch", AttributeValue.fromS(
                mapper.writeValueAsString(new PreSendDispatch(command, null))));
        return value;
    }

    private Map<String, AttributeValue> observed(CarrierHttpResult.Status status) {
        var value = new HashMap<>(command.stepKey());
        value.put("status", AttributeValue.fromS("OBSERVED"));
        value.put("carrier", AttributeValue.fromS(command.carrier().name()));
        value.put("command", AttributeValue.fromS(mapper.writeValueAsString(command)));
        var result = new CarrierHttpResult(CarrierHttpResult.id(command), ID, command.attemptId(),
                command.carrier(), command.invocation(), "HTTP_RESPONSE", status,
                status == CarrierHttpResult.Status.ACCEPTED ? 200 : 400, null, null,
                status == CarrierHttpResult.Status.ACCEPTED ? null : 66003, null, NOW);
        value.put("http_observation", AttributeValue.fromS(mapper.writeValueAsString(result)));
        return value;
    }

    private void reads(Map<String, AttributeValue> origin, Map<String, AttributeValue> step) {
        when(db.getItem(any(GetItemRequest.class))).thenAnswer(call -> GetItemResponse.builder().item(
                call.getArgument(0, GetItemRequest.class).tableName().equals(
                        messaging.common.dynamodb.DynamoDbTableNames.ORIGIN) ? origin : step).build());
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());
    }
}

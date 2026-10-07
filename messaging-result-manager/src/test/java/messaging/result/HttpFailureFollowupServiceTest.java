package messaging.result;

import messaging.common.messages.CarrierHttpResult;
import messaging.common.messages.FollowupHttpCommand;
import messaging.common.messages.HttpCarrier;
import messaging.common.messages.HttpProviderRequest;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageSubmission;
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

class HttpFailureFollowupServiceTest {
    private static final Instant NOW = Instant.parse("2026-10-07T00:01:00Z");
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final FollowupHttpCommandStore commands = mock(FollowupHttpCommandStore.class);
    private final HttpFailureFollowupService service = new HttpFailureFollowupService(db, mapper, commands,
            Clock.fixed(NOW, ZoneOffset.UTC));
    private final MessageSubmission admission = new MessageSubmission("a".repeat(32), 42L, "customer-id",
            "01012345678", MessageCategory.GENERAL, Map.of("text", "hello"), null, NOW.minusSeconds(60));
    private final HttpSendCommand first = command(HttpCarrier.SKT, 1);

    @Test
    void carrierMismatchFreezesTheNextUntriedCarrier() {
        var failure = failed(first, 66001);
        reads(origin(first), observation(first, failure));
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(observation(first, failure)).build());
        when(commands.freeze(eq(admission.clientMsgId()), isNull(), any(FollowupHttpCommand.class)))
                .thenAnswer(call -> call.getArgument(2));

        var stored = assertInstanceOf(HttpFailureFollowupService.FollowupStored.class,
                service.process(failure)).value();

        assertEquals(HttpCarrier.KT, stored.command().carrier());
        assertEquals(HttpSendCommand.attemptId(admission.clientMsgId(), HttpCarrier.KT),
                stored.command().attemptId());
        assertEquals(1, stored.command().invocation());
        assertEquals(NOW, stored.notBefore());
        assertEquals(first.request(), stored.command().request());
        assertEquals(failure.resultId(), stored.decisionId());
    }

    @Test
    void tpsFailureReservesTheSameCarrierOneMinuteLater() {
        var failure = failed(first, 66002);
        reads(origin(first), observation(first, failure));
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(observation(first, failure)).build());
        when(commands.freeze(eq(admission.clientMsgId()), isNull(), any(FollowupHttpCommand.class)))
                .thenAnswer(call -> call.getArgument(2));

        var stored = assertInstanceOf(HttpFailureFollowupService.FollowupStored.class,
                service.process(failure)).value();

        assertEquals(HttpCarrier.SKT, stored.command().carrier());
        assertEquals(2, stored.command().invocation());
        assertEquals(NOW.plusSeconds(60), stored.notBefore());
    }

    @Test
    void carrierMismatchSkipsEveryPreviouslyObservedCarrier() {
        var current = command(HttpCarrier.KT, 1);
        var failure = failed(current, 66001);
        var firstFailure = failed(first, 66001);
        var origin = origin(first);
        origin.put(FollowupHttpCommand.CURRENT_DECISION, AttributeValue.fromS(firstFailure.resultId()));
        origin.put(FollowupHttpCommand.CURRENT_COMMAND, AttributeValue.fromS(mapper.writeValueAsString(current)));
        reads(origin, observation(current, failure));
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(observation(first, firstFailure), observation(current, failure)).build());
        when(commands.freeze(eq(admission.clientMsgId()), eq(firstFailure.resultId()),
                any(FollowupHttpCommand.class))).thenAnswer(call -> call.getArgument(2));

        var stored = assertInstanceOf(HttpFailureFollowupService.FollowupStored.class,
                service.process(failure)).value();

        assertEquals(HttpCarrier.LGU, stored.command().carrier());
        assertEquals(1, stored.command().invocation());
    }

    @Test
    void timeoutReservesTheSameCarrierOneMinuteLaterWithoutProviderCode() {
        var timeout = new CarrierHttpResult(CarrierHttpResult.id(first), admission.clientMsgId(),
                first.attemptId(), first.carrier(), first.invocation(), "HTTP_TIMEOUT",
                CarrierHttpResult.Status.TIMEOUT, null, null, null, null, null, NOW);
        reads(origin(first), observation(first, timeout));
        when(commands.freeze(eq(admission.clientMsgId()), isNull(), any(FollowupHttpCommand.class)))
                .thenAnswer(call -> call.getArgument(2));

        var stored = assertInstanceOf(HttpFailureFollowupService.FollowupStored.class,
                service.process(timeout)).value();

        assertEquals(HttpCarrier.SKT, stored.command().carrier());
        assertEquals(2, stored.command().invocation());
        assertEquals(NOW.plusSeconds(60), stored.notBefore());
        verify(db, never()).query(any(QueryRequest.class));
    }

    @Test
    void staleResultCannotAdvanceTheCurrentDecision() {
        var failure = failed(first, 66001);
        var current = command(HttpCarrier.KT, 1);
        var origin = origin(first);
        origin.put(FollowupHttpCommand.CURRENT_DECISION, AttributeValue.fromS("previous-result"));
        origin.put(FollowupHttpCommand.CURRENT_COMMAND, AttributeValue.fromS(mapper.writeValueAsString(current)));
        reads(origin, Map.of());

        assertInstanceOf(HttpFailureFollowupService.Ignored.class, service.process(failure));
        verifyNoInteractions(commands);
        verify(db, never()).query(any(QueryRequest.class));
    }

    @Test
    void replayOfTheResultThatAlreadyAuthorizedTheNextCommandIsIgnored() {
        var failure = failed(first, 66002);
        var next = command(HttpCarrier.SKT, 2);
        var origin = origin(first);
        origin.put(FollowupHttpCommand.CURRENT_DECISION, AttributeValue.fromS(failure.resultId()));
        origin.put(FollowupHttpCommand.CURRENT_COMMAND, AttributeValue.fromS(mapper.writeValueAsString(next)));
        reads(origin, Map.of());

        assertInstanceOf(HttpFailureFollowupService.Ignored.class, service.process(failure));
        verifyNoInteractions(commands);
    }

    @Test
    void unverifiedKafkaResultCannotTriggerAnotherProviderCall() {
        var failure = failed(first, 66001);
        reads(origin(first), Map.of());

        assertThrows(IllegalStateException.class, () -> service.process(failure));
        verifyNoInteractions(commands);
    }

    @Test
    void unrelatedProviderFailureIsReturnedForPrimaryFailureHandling() {
        var failure = failed(first, 66003);
        reads(origin(first), observation(first, failure));
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(observation(first, failure)).build());

        var pending = assertInstanceOf(HttpFailureFollowupService.PrimaryFailurePending.class,
                service.process(failure));

        assertEquals(66003, pending.errorCode());
        verifyNoInteractions(commands);
    }

    @Test
    void retryThatWouldMissTheFirstSendDeadlineIsNotFrozen() {
        var soonExpired = new HttpSendCommand(first.attemptId(), first.carrier(), 1,
                NOW.plusSeconds(30), first.request());
        var failure = failed(soonExpired, 66002);
        reads(origin(soonExpired), observation(soonExpired, failure));
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(observation(soonExpired, failure)).build());

        assertInstanceOf(HttpFailureFollowupService.Expired.class, service.process(failure));
        verifyNoInteractions(commands);
    }

    private HttpSendCommand command(HttpCarrier carrier, int invocation) {
        return new HttpSendCommand(HttpSendCommand.attemptId(admission.clientMsgId(), carrier), carrier,
                invocation, NOW.plusSeconds(3600), new HttpProviderRequest(admission.clientMsgId(), 42L,
                "GENERAL", "01012345678", admission.payload(), admission.receivedAt()));
    }

    private CarrierHttpResult failed(HttpSendCommand command, int code) {
        return new CarrierHttpResult(CarrierHttpResult.id(command), admission.clientMsgId(),
                command.attemptId(), command.carrier(), command.invocation(), "HTTP_RESPONSE",
                CarrierHttpResult.Status.FAILED, 400, "4xx", "41001", code, "provider failure", NOW);
    }

    private Map<String, AttributeValue> origin(HttpSendCommand initial) {
        var item = new HashMap<>(MessageOriginCodec.encode(admission, mapper));
        item.put("pre_send_dispatch", AttributeValue.fromS(
                mapper.writeValueAsString(new PreSendDispatch(initial, null))));
        return item;
    }

    private Map<String, AttributeValue> observation(HttpSendCommand command, CarrierHttpResult result) {
        var item = new HashMap<>(command.stepKey());
        item.put("status", AttributeValue.fromS("OBSERVED"));
        item.put("carrier", AttributeValue.fromS(command.carrier().name()));
        item.put("command", AttributeValue.fromS(mapper.writeValueAsString(command)));
        item.put("http_observation", AttributeValue.fromS(mapper.writeValueAsString(result)));
        return item;
    }

    private void reads(Map<String, AttributeValue> origin, Map<String, AttributeValue> step) {
        when(db.getItem(any(GetItemRequest.class))).thenAnswer(call -> GetItemResponse.builder().item(
                call.getArgument(0, GetItemRequest.class).tableName().equals(
                        messaging.common.dynamodb.DynamoDbTableNames.ORIGIN) ? origin : step).build());
    }
}

package messaging.result;

import messaging.common.messages.CarrierHttpResult;
import messaging.common.messages.HttpCarrier;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageResultInboxIndex;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class PendingResultDispatcherTest {
    private final io.micrometer.core.instrument.simple.SimpleMeterRegistry metrics =
            new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
    private static final Instant NOW = Instant.parse("2026-10-07T00:01:00Z");
    private final MessageResultInboxStore inbox = mock(MessageResultInboxStore.class);
    private final WebhookPrimaryDecisionService decisions = mock(WebhookPrimaryDecisionService.class);
    private final HttpFailureFollowupService http = mock(HttpFailureFollowupService.class);
    private final PrimaryStageDecisionStore terminal = mock(PrimaryStageDecisionStore.class);
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final PendingResultDispatcher dispatcher = new PendingResultDispatcher(db,
            inbox, decisions, http, terminal, mock(SecondaryResultService.class),
            mapper, Clock.fixed(NOW, ZoneOffset.UTC), 100, metrics);
    private final MessageResultInboxStore.Item item = new MessageResultInboxStore.Item("a".repeat(32),
            "result-1", "WEBHOOK", "{}", NOW);
    private final Map<String, AttributeValue> key = item.key();

    @Test
    void measuresOnlyInboxWorkOnTheSharedIndexAndContinuesAfterItemFailure() {
        var candidate = new java.util.HashMap<>(key);
        candidate.put(MessageResultInboxIndex.DUE, AttributeValue.fromN(Long.toString(NOW.minusSeconds(7).toEpochMilli())));
        var unrelated = Map.of("pk", key.get("pk"), "sk", AttributeValue.fromS("PRIMARY_DECISION#other"));
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().items(candidate, unrelated).build(),
                QueryResponse.builder().build());
        when(inbox.loadPending(key, NOW)).thenThrow(new IllegalStateException("test failure"));

        dispatcher.poll();

        verify(inbox).loadPending(key, NOW);
        verify(db, times(MessageResultInboxIndex.SHARDS)).query(any(QueryRequest.class));
        assertEquals(1, metrics.get("messaging.scheduled.work.duration").tag("phase", "item").timer().count());
        assertEquals(1, metrics.get("messaging.scheduled.work.duration").tag("phase", "poll").timer().count());
        assertEquals(7, metrics.get("messaging.scheduled.work.due.age").timer().totalTime(java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(2, metrics.get("messaging.scheduled.work.page.items").tag("kind", "candidate").summary().totalAmount());
        assertEquals(1, metrics.get("messaging.scheduled.work.page.items").tag("kind", "eligible").summary().totalAmount());
    }

    @Test
    void firstPageQueriesOmitExclusiveStartKey() {
        when(db.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());

        dispatcher.poll();

        var requests = org.mockito.ArgumentCaptor.forClass(QueryRequest.class);
        verify(db, times(MessageResultInboxIndex.SHARDS)).query(requests.capture());
        requests.getAllValues().forEach(request -> assertFalse(request.hasExclusiveStartKey()));
    }

    @Test
    void acceptedFailureFollowupIsMarkedProcessed() {
        when(inbox.loadPending(key, NOW)).thenReturn(item);
        when(decisions.process(item)).thenReturn(new WebhookPrimaryDecisionService.Ignored());

        dispatcher.dispatch(key, NOW);

        verify(inbox).processed(item);
        verify(inbox, never()).defer(any(), any());
    }

    @Test
    void webhookThatPrecedesHttpObservationIsRetriedSoon() {
        when(inbox.loadPending(key, NOW)).thenReturn(item);
        when(decisions.process(item)).thenReturn(new WebhookPrimaryDecisionService.AwaitingHttp());

        dispatcher.dispatch(key, NOW);

        verify(inbox).defer(item, NOW.plusSeconds(5));
        verify(inbox, never()).processed(any());
    }

    @Test
    void successFreezesThePrimaryStageAndCompletesTheInboxItem() {
        when(inbox.loadPending(key, NOW)).thenReturn(item);
        var command = mock(HttpSendCommand.class);
        when(decisions.process(item)).thenReturn(new WebhookPrimaryDecisionService.SuccessPending(null, command));

        dispatcher.dispatch(key, NOW);

        verify(terminal).fromWebhook(item, null, command, null);
        verify(inbox).processed(item);
    }

    @Test
    void exhaustedHttpFailureFreezesThePrimaryStage() {
        var result = new CarrierHttpResult("result-http", item.clientMsgId(), "attempt-1", HttpCarrier.SKT,
                4, "HTTP_RESPONSE", CarrierHttpResult.Status.FAILED, 400, "4xx", "41001", 66002,
                "tps", NOW);
        var httpItem = new MessageResultInboxStore.Item(item.clientMsgId(), "result-http", "HTTP_RESPONSE",
                mapper.writeValueAsString(result), NOW);
        var failure = new HttpFailureFollowupService.PrimaryFailurePending(40001, null,
                mock(HttpSendCommand.class));
        when(inbox.loadPending(httpItem.key(), NOW)).thenReturn(httpItem);
        when(http.process(result)).thenReturn(failure);

        dispatcher.dispatch(httpItem.key(), NOW);

        verify(terminal).fromHttp(httpItem, failure);
        verify(inbox).processed(httpItem);
    }

    @Test
    void timeoutRemainsPendingWhileAnEarlierWebhookIsUndecided() {
        var result = new CarrierHttpResult("timeout", item.clientMsgId(), "attempt-1", HttpCarrier.SKT,
                1, "HTTP_TIMEOUT", CarrierHttpResult.Status.TIMEOUT, null, null, null, null, null, NOW);
        var httpItem = new MessageResultInboxStore.Item(item.clientMsgId(), "timeout", "HTTP_TIMEOUT",
                mapper.writeValueAsString(result), NOW);
        when(inbox.loadPending(httpItem.key(), NOW)).thenReturn(httpItem);
        when(http.process(result)).thenReturn(new HttpFailureFollowupService.AwaitingWebhook());

        dispatcher.dispatch(httpItem.key(), NOW);

        verify(inbox).defer(httpItem, NOW.plusSeconds(5));
        verify(inbox, never()).processed(any());
        verifyNoInteractions(terminal);
    }

    @Test
    void preSendFailureFreezesThePrimaryStage() {
        var preSend = new MessageResultInboxStore.Item(item.clientMsgId(), "result-pre", "PRE_SEND", "{}", NOW);
        when(inbox.loadPending(preSend.key(), NOW)).thenReturn(preSend);

        dispatcher.dispatch(preSend.key(), NOW);

        verify(terminal).fromPreSend(preSend);
        verify(inbox).processed(preSend);
    }
}

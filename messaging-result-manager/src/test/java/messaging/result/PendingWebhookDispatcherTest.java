package messaging.result;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.mockito.Mockito.*;

class PendingWebhookDispatcherTest {
    private static final Instant NOW = Instant.parse("2026-10-07T00:01:00Z");
    private final MessageResultInboxStore inbox = mock(MessageResultInboxStore.class);
    private final WebhookPrimaryDecisionService decisions = mock(WebhookPrimaryDecisionService.class);
    private final PendingWebhookDispatcher dispatcher = new PendingWebhookDispatcher(mock(DynamoDbClient.class),
            inbox, decisions, Clock.fixed(NOW, ZoneOffset.UTC), 100);
    private final MessageResultInboxStore.Item item = new MessageResultInboxStore.Item("a".repeat(32),
            "result-1", "WEBHOOK", "{}", NOW);
    private final Map<String, AttributeValue> key = item.key();

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
    void successStaysPendingForFinalization() {
        when(inbox.loadPending(key, NOW)).thenReturn(item);
        when(decisions.process(item)).thenReturn(new WebhookPrimaryDecisionService.SuccessPending());

        dispatcher.dispatch(key, NOW);

        verify(inbox).defer(item, NOW.plusSeconds(60));
        verify(inbox, never()).processed(any());
    }
}

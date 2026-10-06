package messaging.http.sender;

import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageTopics;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.HttpOutcome;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class HttpSendServiceTest {
    private final HttpAttemptRepository attempts = mock(HttpAttemptRepository.class);
    private final HttpProviderClient provider = mock(HttpProviderClient.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, HttpOutcome> kafka = mock(KafkaTemplate.class);
    private final HttpSenderProperties properties = new HttpSenderProperties("mock-provider", "http://localhost:8090",
            Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(2), 64,
            Duration.ofHours(3), Duration.ofSeconds(30));
    private final Clock clock = Clock.fixed(Instant.parse("2026-10-02T00:01:00Z"), ZoneOffset.UTC);
    private final HttpSendService service = new HttpSendService(attempts, provider, kafka, properties, clock);
    private final MessageSubmission event = new MessageSubmission("00000000-0000-0000-0000-000000000001", 42,
            "customer-1", "01012345678", MessageCategory.GENERAL, Map.of("message", "hello"), false,
            Instant.parse("2026-10-02T00:00:00Z"));

    @Test
    void persistsProviderObservationBeforePublishingIt() {
        when(attempts.claim(eq(event), eq("mock-provider"), any(), any(), any()))
                .thenReturn(new HttpAttemptRepository.Claim(HttpAttemptRepository.State.CLAIMED, "attempt-1", null));
        when(provider.send(event, "attempt-1", 1)).thenReturn(
                new HttpProviderClient.Observation(HttpOutcome.Kind.ACCEPTED, clock.instant()));
        when(attempts.record(any())).thenAnswer(call -> call.getArgument(0));
        when(kafka.send(eq(MessageTopics.HTTP_OUTCOME), eq(event.clientMsgId()), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        service.send(event);

        var order = inOrder(attempts, provider, kafka);
        order.verify(attempts).claim(eq(event), eq("mock-provider"), any(), any(), any());
        order.verify(provider).send(event, "attempt-1", 1);
        order.verify(attempts).record(any(HttpOutcome.class));
        order.verify(kafka).send(eq(MessageTopics.HTTP_OUTCOME), eq(event.clientMsgId()), any(HttpOutcome.class));
        order.verify(attempts).published(any(HttpOutcome.class));
    }

    @Test
    void duplicateWithStoredOutcomeRepublishesWithoutCallingProvider() {
        var stored = new HttpOutcome("outcome-1", event.clientMsgId(), "attempt-1", 1, 1,
                HttpOutcome.Kind.ACCEPTED, clock.instant(), clock.instant());
        when(attempts.claim(eq(event), eq("mock-provider"), any(), any(), any()))
                .thenReturn(new HttpAttemptRepository.Claim(HttpAttemptRepository.State.OBSERVED, "attempt-1", stored));
        when(kafka.send(MessageTopics.HTTP_OUTCOME, event.clientMsgId(), stored))
                .thenReturn(CompletableFuture.completedFuture(null));

        service.send(event);

        verifyNoInteractions(provider);
        verify(attempts, never()).record(any());
        verify(kafka).send(MessageTopics.HTTP_OUTCOME, event.clientMsgId(), stored);
    }

    @Test
    void claimedAfterDeadlineRecordsExpiryWithoutExternalCall() {
        var expired = new MessageSubmission(event.clientMsgId(), event.clientId(), event.messageId(),
                event.recipientNumber(), event.messageCategory(), event.payload(), event.fallbackAllowed(),
                clock.instant().minus(Duration.ofHours(4)));
        when(attempts.claim(eq(expired), eq("mock-provider"), any(), any(), any()))
                .thenReturn(new HttpAttemptRepository.Claim(HttpAttemptRepository.State.CLAIMED, "attempt-1", null));
        when(kafka.send(eq(MessageTopics.HTTP_OUTCOME), eq(expired.clientMsgId()), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        service.send(expired);

        verifyNoInteractions(provider);
        var captured = org.mockito.ArgumentCaptor.forClass(HttpOutcome.class);
        verify(attempts).record(captured.capture());
        assertEquals(HttpOutcome.Kind.EXPIRED, captured.getValue().kind());
    }

    @Test
    void inProgressClaimNeverCallsProvider() {
        when(attempts.claim(eq(event), eq("mock-provider"), any(), any(), any()))
                .thenReturn(new HttpAttemptRepository.Claim(HttpAttemptRepository.State.IN_PROGRESS, "attempt-1", null));

        service.send(event);

        verifyNoInteractions(provider, kafka);
    }
}

package event.http.sender;

import event.common.events.EventSubmission;
import event.common.events.EventTopics;
import event.common.events.EventType;
import event.common.events.HttpOutcome;
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
    private final EventSubmission event = new EventSubmission("00000000-0000-0000-0000-000000000001", 42,
            "customer-1", "01012345678", EventType.GENERAL, Map.of("message", "hello"), false,
            Instant.parse("2026-10-02T00:00:00Z"));

    @Test
    void persistsProviderObservationBeforePublishingIt() {
        when(attempts.claim(eq(event), eq("mock-provider"), any(), any(), any()))
                .thenReturn(new HttpAttemptRepository.Claim(HttpAttemptRepository.State.CLAIMED, "attempt-1", null));
        when(provider.send(event, "attempt-1", 1)).thenReturn(
                new HttpProviderClient.Observation(HttpOutcome.Kind.ACCEPTED, clock.instant()));
        when(attempts.record(any())).thenAnswer(call -> call.getArgument(0));
        when(kafka.send(eq(EventTopics.HTTP_OUTCOME), eq(event.executionId()), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        service.send(event);

        var order = inOrder(attempts, provider, kafka);
        order.verify(attempts).claim(eq(event), eq("mock-provider"), any(), any(), any());
        order.verify(provider).send(event, "attempt-1", 1);
        order.verify(attempts).record(any(HttpOutcome.class));
        order.verify(kafka).send(eq(EventTopics.HTTP_OUTCOME), eq(event.executionId()), any(HttpOutcome.class));
        order.verify(attempts).published(any(HttpOutcome.class));
    }

    @Test
    void duplicateWithStoredOutcomeRepublishesWithoutCallingProvider() {
        var stored = new HttpOutcome("outcome-1", event.executionId(), "attempt-1", 1, 1,
                HttpOutcome.Kind.ACCEPTED, clock.instant(), clock.instant());
        when(attempts.claim(eq(event), eq("mock-provider"), any(), any(), any()))
                .thenReturn(new HttpAttemptRepository.Claim(HttpAttemptRepository.State.OBSERVED, "attempt-1", stored));
        when(kafka.send(EventTopics.HTTP_OUTCOME, event.executionId(), stored))
                .thenReturn(CompletableFuture.completedFuture(null));

        service.send(event);

        verifyNoInteractions(provider);
        verify(attempts, never()).record(any());
        verify(kafka).send(EventTopics.HTTP_OUTCOME, event.executionId(), stored);
    }

    @Test
    void claimedAfterDeadlineRecordsExpiryWithoutExternalCall() {
        var expired = new EventSubmission(event.executionId(), event.clientId(), event.eventId(),
                event.recipientNumber(), event.eventType(), event.payload(), event.fallbackAllowed(),
                clock.instant().minus(Duration.ofHours(4)));
        when(attempts.claim(eq(expired), eq("mock-provider"), any(), any(), any()))
                .thenReturn(new HttpAttemptRepository.Claim(HttpAttemptRepository.State.CLAIMED, "attempt-1", null));
        when(kafka.send(eq(EventTopics.HTTP_OUTCOME), eq(expired.executionId()), any()))
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

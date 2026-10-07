package messaging.carrier.sender;

import messaging.common.messages.CarrierHttpResult;
import messaging.common.messages.HttpCarrier;
import messaging.common.messages.HttpProviderRequest;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageTopics;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CarrierHttpSendServiceTest {
    private static final Instant NOW = Instant.parse("2026-10-07T00:00:00Z");
    private final HttpSendCommand command = new HttpSendCommand("attempt-kt", HttpCarrier.KT, 1,
            NOW.plusSeconds(3600), new HttpProviderRequest("a".repeat(32), 42L, "GENERAL",
            "01012345678", Map.of("text", "hello"), NOW));
    private final CarrierSendGate gate = mock(CarrierSendGate.class);
    private final CarrierHttpAttemptStore attempts = mock(CarrierHttpAttemptStore.class);
    private final CarrierProviderClient provider = mock(CarrierProviderClient.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, CarrierHttpResult> kafka = mock(KafkaTemplate.class);
    private final CarrierHttpSendService service = new CarrierHttpSendService(gate, attempts, provider,
            new CarrierErrorNormalizer(Set.of("41001"), Set.of("42002")), kafka,
            Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(30));

    @Test
    void http200RecordsAcceptanceWithoutPublishingKafka() throws Exception {
        when(gate.claim(command)).thenReturn(CarrierSendGate.Decision.SEND);
        when(provider.send(command)).thenReturn(new CarrierProviderReply.Accepted());

        service.send(command);

        verify(attempts).record(eq(command), argThat(result -> result.status() == CarrierHttpResult.Status.ACCEPTED
                && result.httpStatus() == 200 && !result.needsPublication()));
        verifyNoInteractions(kafka);
    }

    @Test
    void explicitFailurePreservesProviderCodeAndPublishesNormalizedCode() throws Exception {
        when(gate.claim(command)).thenReturn(CarrierSendGate.Decision.SEND);
        when(provider.send(command)).thenReturn(new CarrierProviderReply.Failed(400, "400", "41001", "not our carrier"));
        when(kafka.send(eq(MessageTopics.MSG_RESULT), eq(command.request().clientMsgId()), any(CarrierHttpResult.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        service.send(command);

        var result = org.mockito.ArgumentCaptor.forClass(CarrierHttpResult.class);
        verify(attempts).record(eq(command), result.capture());
        assertEquals("41001", result.getValue().providerErrorCode());
        assertEquals(66001, result.getValue().normalizedErrorCode());
        assertEquals("HTTP_RESPONSE", result.getValue().source());
        verify(kafka).send(MessageTopics.MSG_RESULT, command.request().clientMsgId(), result.getValue());
        verify(attempts).published(command, result.getValue());
    }

    @Test
    void failedKafkaAckReplaysTheStoredResultWithoutAnotherProviderCall() throws Exception {
        var result = new CarrierHttpResult(CarrierHttpResult.id(command), command.request().clientMsgId(),
                command.attemptId(), command.carrier(), 1, "HTTP_TIMEOUT", CarrierHttpResult.Status.TIMEOUT,
                null, null, null, null, null, NOW);
        when(gate.claim(command)).thenReturn(CarrierSendGate.Decision.OBSERVED);
        when(attempts.observation(command)).thenReturn(Optional.of(result));
        var failed = new CompletableFuture<org.springframework.kafka.support.SendResult<String, CarrierHttpResult>>();
        failed.completeExceptionally(new IllegalStateException("Kafka unavailable"));
        when(kafka.send(MessageTopics.MSG_RESULT, command.request().clientMsgId(), result))
                .thenReturn(failed, CompletableFuture.completedFuture(null));

        assertThrows(ExecutionException.class, () -> service.send(command));
        service.send(command);

        verifyNoInteractions(provider);
        verify(kafka, times(2)).send(MessageTopics.MSG_RESULT, command.request().clientMsgId(), result);
        verify(attempts, times(1)).published(command, result);
    }

    @Test
    void staleInProgressCallBecomesTimeoutWithoutCallingProviderAgain() throws Exception {
        when(gate.claim(command)).thenReturn(CarrierSendGate.Decision.IN_PROGRESS);
        var result = new CarrierHttpResult(CarrierHttpResult.id(command), command.request().clientMsgId(),
                command.attemptId(), command.carrier(), 1, "HTTP_TIMEOUT", CarrierHttpResult.Status.TIMEOUT,
                null, null, null, null, null, NOW);
        when(attempts.recoverStale(command, NOW, NOW.minusSeconds(30))).thenReturn(Optional.of(result));
        when(kafka.send(MessageTopics.MSG_RESULT, command.request().clientMsgId(), result))
                .thenReturn(CompletableFuture.completedFuture(null));

        service.send(command);

        verifyNoInteractions(provider);
        verify(attempts).published(command, result);
    }
}

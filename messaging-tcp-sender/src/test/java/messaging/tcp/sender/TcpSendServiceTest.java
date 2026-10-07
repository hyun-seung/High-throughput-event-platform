package messaging.tcp.sender;

import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageTopics;
import messaging.common.messages.PrimaryStageDecision;
import messaging.common.messages.RedisSendAttemptGuard;
import messaging.common.messages.SecondarySendCommand;
import messaging.common.messages.TcpSendResult;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TcpSendServiceTest {
    private static final Instant NOW = Instant.parse("2026-10-07T12:00:00Z");
    private static final String ID = "b".repeat(32);
    private final SecondarySendCommand command = command();
    private final TcpSendResult result = new TcpSendResult(TcpSendResult.id(ID, command.attemptId()),
            ID, command.attemptId(), "TCP_RESPONSE", TcpSendResult.Status.SUCCESS, null, "RECEIVED", NOW);
    private final TcpAttemptStore attempts = mock(TcpAttemptStore.class);
    private final RedisSendAttemptGuard redis = mock(RedisSendAttemptGuard.class);
    private final TcpProviderClient provider = mock(TcpProviderClient.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, TcpSendResult> kafka = mock(KafkaTemplate.class);
    private final TcpSenderProperties settings = new TcpSenderProperties(true, "127.0.0.1", 18091,
            Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofSeconds(30));
    private final TcpSendService service = new TcpSendService(attempts, redis, provider, kafka,
            settings, Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void newCommandClaimsBeforeCallAndPublishesStoredResult() throws Exception {
        when(attempts.reserve(command, NOW)).thenReturn(TcpAttemptStore.State.RESERVED);
        when(redis.claimTcp(ID, command.attemptId(), settings.claimTtl())).thenReturn(true);
        when(attempts.begin(command, NOW)).thenReturn(true);
        when(provider.send(command)).thenReturn(result);
        when(attempts.record(command, result)).thenReturn(result);
        when(attempts.publicationPending(command)).thenReturn(true);
        when(kafka.send(MessageTopics.MSG_RESULT, ID, result)).thenReturn(CompletableFuture.completedFuture(null));

        service.send(command);

        var order = inOrder(attempts, redis, provider, kafka);
        order.verify(attempts).reserve(command, NOW);
        order.verify(redis).claimTcp(ID, command.attemptId(), settings.claimTtl());
        order.verify(attempts).begin(command, NOW);
        order.verify(provider).send(command);
        order.verify(attempts).record(command, result);
        order.verify(kafka).send(MessageTopics.MSG_RESULT, ID, result);
        order.verify(attempts).published(command);
    }

    @Test
    void replayOfObservedCommandOnlyRepublishesFrozenResult() throws Exception {
        when(attempts.reserve(command, NOW)).thenReturn(TcpAttemptStore.State.OBSERVED);
        when(attempts.observation(command)).thenReturn(Optional.of(result));
        when(attempts.publicationPending(command)).thenReturn(true);
        when(kafka.send(MessageTopics.MSG_RESULT, ID, result)).thenReturn(CompletableFuture.completedFuture(null));

        service.send(command);

        verifyNoInteractions(redis, provider);
        verify(kafka).send(MessageTopics.MSG_RESULT, ID, result);
        verify(attempts).published(command);
    }

    private static SecondarySendCommand command() {
        var decision = new PrimaryStageDecision("primary-failed", ID, PrimaryStageDecision.Kind.FAILURE,
                "HTTP_RESPONSE", 66003, null, null, null, true, NOW);
        var submission = new MessageSubmission(ID, 42L, "customer-1", "01012345678",
                MessageCategory.GENERAL, Map.of("text", "primary"), Map.of("text", "secondary"), NOW);
        return SecondarySendCommand.from(decision, submission);
    }
}

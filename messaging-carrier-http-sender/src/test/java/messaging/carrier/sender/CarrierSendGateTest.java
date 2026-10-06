package messaging.carrier.sender;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.HttpProviderRequest;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.RedisSendAttemptGuard;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CarrierSendGateTest {
    private static final Instant NOW = Instant.parse("2026-10-07T00:00:00Z");
    private final CarrierHttpAttemptStore attempts = mock(CarrierHttpAttemptStore.class);
    private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> values = mock(ValueOperations.class);
    private final RedisSendAttemptGuard redis = new RedisSendAttemptGuard(redisTemplate);
    private final CarrierSendGate gate = new CarrierSendGate(HttpCarrier.KT, attempts, redis,
            Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofMinutes(10));
    private final HttpSendCommand command = new HttpSendCommand("attempt-kt", HttpCarrier.KT, 1,
            NOW.plusSeconds(3600), new HttpProviderRequest("a".repeat(32), 42L, "GENERAL",
            "01012345678", Map.of("text", "hello"), NOW));

    @Test
    void wrongCarrierCannotClaimOrSend() {
        var sktCommand = new HttpSendCommand(command.attemptId(), HttpCarrier.SKT, 1,
                command.deadlineAt(), command.request());
        assertThrows(IllegalArgumentException.class, () -> gate.claim(sktCommand));
        verifyNoInteractions(attempts, redisTemplate);
    }

    @Test
    void onlyTheRedisWinnerCanTransitionToSending() {
        when(attempts.reserve(command, NOW)).thenReturn(CarrierHttpAttemptStore.State.PENDING);
        when(redisTemplate.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(anyString(), eq("1"), eq(Duration.ofMinutes(10)))).thenReturn(false, true);
        when(attempts.begin(command, NOW)).thenReturn(true);

        assertEquals(CarrierSendGate.Decision.WAIT, gate.claim(command));
        assertEquals(CarrierSendGate.Decision.SEND, gate.claim(command));
        verify(attempts, times(1)).begin(command, NOW);
    }

    @Test
    void aPreviouslyStartedCallCannotBeSentAgainAfterRedisLoss() {
        when(attempts.reserve(command, NOW)).thenReturn(CarrierHttpAttemptStore.State.SENDING);
        assertEquals(CarrierSendGate.Decision.WAIT, gate.claim(command));
        verifyNoInteractions(redisTemplate);
    }

    @Test
    void expiredCommandDoesNotTouchStorage() {
        var expired = new HttpSendCommand(command.attemptId(), command.carrier(), 1, NOW, command.request());
        assertEquals(CarrierSendGate.Decision.INELIGIBLE, gate.claim(expired));
        verify(attempts).state(expired);
        verify(attempts, never()).reserve(any(), any());
        verifyNoInteractions(redisTemplate);
    }

    @Test
    void anObservedFailureRemainsPublishableAfterTheSendDeadline() {
        var expired = new HttpSendCommand(command.attemptId(), command.carrier(), 1, NOW, command.request());
        when(attempts.state(expired)).thenReturn(CarrierHttpAttemptStore.State.OBSERVED);
        assertEquals(CarrierSendGate.Decision.OBSERVED, gate.claim(expired));
        verifyNoInteractions(redisTemplate);
    }
}

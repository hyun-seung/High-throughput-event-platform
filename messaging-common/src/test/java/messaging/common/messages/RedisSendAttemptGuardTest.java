package messaging.common.messages;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisSendAttemptGuardTest {
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final ValueOperations<String, String> values = mock(ValueOperations.class);
    private final RedisSendAttemptGuard guard = new RedisSendAttemptGuard(redis);
    private final HttpProviderRequest request = new HttpProviderRequest("a".repeat(32), 42L, "GENERAL",
            "01012345678", Map.of("message", "hello"), Instant.parse("2026-10-07T00:00:00Z"));

    @Test
    void sameHttpCallSharesAClaimButRetryAndCarrierMoveUseDifferentKeys() {
        var first = new HttpSendCommand("attempt-skt", HttpCarrier.SKT, 1,
                request.occurredAt().plusSeconds(3600), request);
        var retry = new HttpSendCommand("attempt-skt", HttpCarrier.SKT, 2,
                first.deadlineAt(), request);
        var moved = new HttpSendCommand("attempt-kt", HttpCarrier.KT, 1,
                first.deadlineAt(), request);
        String firstKey = "message:http:attempt:" + request.clientMsgId() + ":SKT:attempt-skt:1";
        String retryKey = "message:http:attempt:" + request.clientMsgId() + ":SKT:attempt-skt:2";
        String movedKey = "message:http:attempt:" + request.clientMsgId() + ":KT:attempt-kt:1";

        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(firstKey, "1", Duration.ofSeconds(30)))
                .thenReturn(true, false);
        when(values.setIfAbsent(retryKey, "1", Duration.ofSeconds(30))).thenReturn(true);
        when(values.setIfAbsent(movedKey, "1", Duration.ofSeconds(30))).thenReturn(true);
        assertTrue(guard.claimHttp(first, Duration.ofSeconds(30)));
        assertFalse(guard.claimHttp(first, Duration.ofSeconds(30)));
        assertTrue(guard.claimHttp(retry, Duration.ofSeconds(30)));
        assertTrue(guard.claimHttp(moved, Duration.ofSeconds(30)));
        verify(values, times(2)).setIfAbsent(firstKey, "1", Duration.ofSeconds(30));
        verify(values).setIfAbsent(retryKey, "1", Duration.ofSeconds(30));
        verify(values).setIfAbsent(movedKey, "1", Duration.ofSeconds(30));
    }

    @Test
    void tcpClaimUsesTheSecondaryAttemptAndPropagatesUncertainRedisResult() {
        String first = "message:tcp:attempt:" + request.clientMsgId() + ":tcp-1";
        String next = "message:tcp:attempt:" + request.clientMsgId() + ":tcp-2";
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(first, "1", Duration.ofMinutes(1))).thenReturn(null);
        when(values.setIfAbsent(next, "1", Duration.ofMinutes(1))).thenReturn(true, false);
        assertThrows(IllegalStateException.class,
                () -> guard.claimTcp(request.clientMsgId(), "tcp-1", Duration.ofMinutes(1)));
        assertThrows(IllegalArgumentException.class,
                () -> guard.claimTcp(request.clientMsgId(), "tcp-1", Duration.ZERO));
        assertTrue(guard.claimTcp(request.clientMsgId(), "tcp-2", Duration.ofMinutes(1)));
        assertFalse(guard.claimTcp(request.clientMsgId(), "tcp-2", Duration.ofMinutes(1)));
        verify(values).setIfAbsent(first, "1", Duration.ofMinutes(1));
        verify(values, times(2)).setIfAbsent(next, "1", Duration.ofMinutes(1));
    }
}

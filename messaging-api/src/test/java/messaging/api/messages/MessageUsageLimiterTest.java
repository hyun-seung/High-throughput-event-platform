package messaging.api.messages;

import messaging.common.messages.MessageCategory;
import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpStatus;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MessageUsageLimiterTest {
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final MessageUsageLimiter limiter = new MessageUsageLimiter(redis,
            Clock.fixed(Instant.parse("2026-10-02T15:00:00Z"), ZoneOffset.UTC));

    @Test
    void usesKoreanCalendarMonthAndTypeSpecificQuotaKeyWithoutContractLookup() {
        when(redis.execute(any(RedisScript.class), anyList(), any(String.class)))
                .thenReturn(List.of(0L, 1L, 1L));

        limiter.charge(42L, MessageCategory.ALERT);

        verify(redis).execute(any(RedisScript.class), eq(List.of(
                "message:usage:{client:42}:policy", "message:usage:{client:42}:10s",
                "message:usage:{client:42}:month:2026-10:ALERT")), eq("ALERT"));
    }

    @Test
    void rejectsExceededLimitsAndMissingPolicy() {
        when(redis.execute(any(RedisScript.class), anyList(), any(String.class)))
                .thenReturn(List.of(1L, 11L, 1L))
                .thenReturn(List.of(2L, 1L, 101L))
                .thenReturn(List.of(3L, -1L, -1L));

        assertEquals(HttpStatus.TOO_MANY_REQUESTS,
                assertThrows(MessageAdmissionException.class, () -> limiter.charge(42L, MessageCategory.GENERAL)).status());
        assertEquals(HttpStatus.TOO_MANY_REQUESTS,
                assertThrows(MessageAdmissionException.class, () -> limiter.charge(42L, MessageCategory.GENERAL)).status());
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE,
                assertThrows(MessageAdmissionException.class, () -> limiter.charge(42L, MessageCategory.GENERAL)).status());
    }

    @Test
    void redisTimeoutPreservesAdmissionAvailability() {
        when(redis.execute(any(RedisScript.class), anyList(), any(String.class)))
                .thenThrow(new QueryTimeoutException("redis timeout"));

        assertDoesNotThrow(() -> limiter.charge(42L, MessageCategory.GENERAL));
    }
}

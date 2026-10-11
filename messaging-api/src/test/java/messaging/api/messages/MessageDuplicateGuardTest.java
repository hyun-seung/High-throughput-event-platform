package messaging.api.messages;

import messaging.common.messages.MessageCategory;
import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.HttpStatus;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MessageDuplicateGuardTest {
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> values = mock(ValueOperations.class);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final MessageDuplicateGuard guard = new MessageDuplicateGuard(redis, mapper);

    private MessageReceiveRequest request() {
        return mapper.readValue("""
                {"messageId":"customer-1","recipientNumber":"01012345678","messageCategory":"GENERAL",
                 "payload":{"z":3,"a":{"last":2,"first":[{"z":true,"a":"한글"}]}},
                 "secondarySendPayload":{"z":"fallback","a":1}}
                """, MessageReceiveRequest.class);
    }

    private void stored(String value) {
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(false);
        when(values.get(anyString())).thenReturn(value);
    }

    @Test
    void newMarkerUsesAnExplicitVersionAndTheKnownCanonicalUtf8Digest() {
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);

        var claim = guard.claim(42L, request(), "new-id");

        assertTrue(claim.owner());
        assertEquals("new-id:v1:6dbb5a0988f045dece351fb54455fdb5bd94026e9df386b5975b6d6d9d3e699c", claim.value());
        verify(values).setIfAbsent(eq(claim.key()), eq(claim.value()), eq(Duration.ofHours(24)));
        verifyNoMoreInteractions(values);
    }

    @Test
    void reorderedNestedMapsKeepTheSameFingerprintButReorderedArraysDoNot() {
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
        var first = new MessageReceiveRequest("customer-1", "01012345678", MessageCategory.GENERAL,
                mapper.readValue("{\"z\":3,\"a\":[{\"y\":2,\"x\":1},4]}", Map.class), Map.of("z", 2, "a", 1));
        var reordered = new MessageReceiveRequest(first.messageId(), first.recipientNumber(), first.messageCategory(),
                mapper.readValue("{\"a\":[{\"x\":1,\"y\":2},4],\"z\":3}", Map.class), Map.of("a", 1, "z", 2));
        var reversedArray = new MessageReceiveRequest(first.messageId(), first.recipientNumber(), first.messageCategory(),
                Map.of("a", List.of(4, Map.of("x", 1, "y", 2)), "z", 3), first.secondarySendPayload());

        assertEquals(guard.claim(42L, first, "id").value(), guard.claim(42L, reordered, "id").value());
        assertNotEquals(guard.claim(42L, first, "id").value(), guard.claim(42L, reversedArray, "id").value());
    }

    @Test
    void versionedDuplicateRetainsTheExistingExecutionAndMarker() {
        String marker = "existing-id:v1:6dbb5a0988f045dece351fb54455fdb5bd94026e9df386b5975b6d6d9d3e699c";
        stored(marker);

        var claim = guard.claim(42L, request(), "proposed-id");

        assertFalse(claim.owner());
        assertEquals("existing-id", claim.clientMsgId());
        assertEquals(marker, claim.value());
        verify(values).setIfAbsent(anyString(), anyString(), eq(Duration.ofHours(24)));
        verify(values).get(claim.key());
        verifyNoMoreInteractions(values);
    }

    @Test
    void changedPayloadCategoryOrSecondaryPayloadConflictsWithAVersionedMarker() {
        stored("existing-id:v1:6dbb5a0988f045dece351fb54455fdb5bd94026e9df386b5975b6d6d9d3e699c");
        var original = request();
        for (var changed : List.of(
                new MessageReceiveRequest(original.messageId(), original.recipientNumber(), MessageCategory.ALERT,
                        original.payload(), original.secondarySendPayload()),
                new MessageReceiveRequest(original.messageId(), original.recipientNumber(), original.messageCategory(),
                        Map.of("different", true), original.secondarySendPayload()),
                new MessageReceiveRequest(original.messageId(), original.recipientNumber(), original.messageCategory(),
                        original.payload(), null))) {
            assertEquals(HttpStatus.CONFLICT, assertThrows(MessageAdmissionException.class,
                    () -> guard.claim(42L, changed, "proposed-id")).status());
        }
    }

    @Test
    void legacyHashKeepsItsExecutionForOriginReconciliation() {
        String marker = "legacy-id:" + "0".repeat(64);
        stored(marker);

        var claim = guard.claim(42L, request(), "proposed-id");

        assertFalse(claim.owner());
        assertEquals("legacy-id", claim.clientMsgId());
        assertEquals(marker, claim.value());
        verify(values).setIfAbsent(anyString(), anyString(), eq(Duration.ofHours(24)));
        verify(values).get(claim.key());
        verifyNoMoreInteractions(values);
    }

    @Test
    void missingMalformedOrUnsupportedMarkerNeverCreatesAnotherExecution() {
        for (String marker : new String[]{null, "missing-separator", ":" + "0".repeat(64), "id:invalid", "id:v2:" + "0".repeat(64)}) {
            stored(marker);
            assertEquals(HttpStatus.SERVICE_UNAVAILABLE, assertThrows(MessageAdmissionException.class,
                    () -> guard.claim(42L, request(), "proposed-id")).status());
        }
    }

    @Test
    void redisTimeoutStillUsesTheExistingAvailabilityFallback() {
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class)))
                .thenThrow(new QueryTimeoutException("unavailable"));

        var claim = guard.claim(42L, request(), "proposed-id");

        assertTrue(claim.owner());
        assertFalse(claim.redisActive());
        assertEquals("proposed-id", claim.clientMsgId());
    }
}

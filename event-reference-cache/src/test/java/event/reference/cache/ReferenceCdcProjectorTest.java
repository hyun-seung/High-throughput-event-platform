package event.reference.cache;

import event.common.events.EventReferenceCacheKeys;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ReferenceCdcProjectorTest {
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> values = mock(ValueOperations.class);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final ReferenceCdcProjector projector = new ReferenceCdcProjector(redis, mapper);

    @Test
    void snapshotAndUpdatePopulateTheSamePhoneKey() {
        when(redis.opsForValue()).thenReturn(values);
        String key = json(Map.of("phone_number", "01012345678"));

        projector.apply(ReferenceCdcTopics.PHONE_CARRIERS, key,
                change("r", Map.of("phone_number", "01012345678", "carrier", "SKT")));
        projector.apply(ReferenceCdcTopics.PHONE_CARRIERS, key,
                change("u", Map.of("phone_number", "01012345678", "carrier", "KT")));

        verify(values).set(EventReferenceCacheKeys.phoneCarrier("01012345678"), "SKT");
        verify(values).set(EventReferenceCacheKeys.phoneCarrier("01012345678"), "KT");
    }

    @Test
    void contractRowIsCopiedAndDeleteRemovesTheKey() {
        when(redis.opsForValue()).thenReturn(values);
        String key = json(Map.of("client_id", 42));
        projector.apply(ReferenceCdcTopics.CONTRACTS, key,
                change("c", Map.of("client_id", 42, "enabled", true, "tps_limit", 100)));
        projector.apply(ReferenceCdcTopics.CONTRACTS, key, change("d", null));
        projector.apply(ReferenceCdcTopics.CONTRACTS, key, null);

        verify(values).set(eq(EventReferenceCacheKeys.contract(42)), anyString());
        verify(redis).delete(EventReferenceCacheKeys.contract(42));
        verifyNoMoreInteractions(values);
    }

    @Test
    void rejectsMismatchedKeysBeforeWritingToRedis() {
        assertThrows(IllegalArgumentException.class, () -> projector.apply(ReferenceCdcTopics.PHONE_CARRIERS,
                json(Map.of("phone_number", "01012345678")),
                change("u", Map.of("phone_number", "01099999999", "carrier", "LGU"))));
        verifyNoInteractions(redis);
    }

    @Test
    void redisFailurePropagatesSoKafkaRecordIsRetried() {
        when(redis.opsForValue()).thenReturn(values);
        doThrow(new IllegalStateException("redis down")).when(values)
                .set(EventReferenceCacheKeys.phoneCarrier("01012345678"), "SKT");

        assertThrows(IllegalStateException.class, () -> projector.apply(ReferenceCdcTopics.PHONE_CARRIERS,
                json(Map.of("phone_number", "01012345678")),
                change("c", Map.of("phone_number", "01012345678", "carrier", "SKT"))));
    }

    private String change(String op, Map<String, Object> after) {
        return json(Map.of("op", op, "after", after == null ? Map.of() : after));
    }

    private String json(Object value) { return mapper.writeValueAsString(value); }
}

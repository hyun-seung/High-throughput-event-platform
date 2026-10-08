package messaging.pre.send;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageReferenceCacheKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PreSendReferenceReaderTest {
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final ValueOperations<String, String> values = mock(ValueOperations.class);
    private final PreSendReferenceReader reader = new PreSendReferenceReader(redis, jdbc, JsonMapper.builder().build());

    @BeforeEach
    void setUp() {
        when(redis.opsForValue()).thenReturn(values);
    }

    @Test
    void cachedContractAvoidsPostgresAndRetainsDisabledStatus() {
        when(values.get(MessageReferenceCacheKeys.contract(42))).thenReturn("{\"client_id\":42,\"enabled\":false}");

        assertEquals(Optional.of(new ClientMessageContract(42, false)), reader.findContract(42));
        verifyNoInteractions(jdbc);
    }

    @Test
    void missingContractCacheFallsBackToPostgres() {
        when(jdbc.query(anyString(), any(RowMapper.class), eq(42L)))
                .thenReturn(List.of(new ClientMessageContract(42, true)));

        assertEquals(Optional.of(new ClientMessageContract(42, true)), reader.findContract(42));
        verify(jdbc).query(contains("delivery_results.client_message_contracts"), any(RowMapper.class), eq(42L));
    }

    @Test
    void missingContractInBothStoresRemainsMissing() {
        when(jdbc.query(anyString(), any(RowMapper.class), eq(42L))).thenReturn(List.of());

        assertEquals(Optional.empty(), reader.findContract(42));
    }

    @Test
    void malformedCacheIsNotTreatedAsMissing() {
        when(values.get(MessageReferenceCacheKeys.contract(42))).thenReturn("{\"client_id\":99,\"enabled\":true}");

        assertThrows(IllegalArgumentException.class, () -> reader.findContract(42));
        verifyNoInteractions(jdbc);
    }

    @Test
    void carrierReadsRedisAndDefaultsOnlyOnCacheMiss() {
        String phone = "01012345678";
        when(values.get(MessageReferenceCacheKeys.phoneCarrier(phone))).thenReturn("KT", null, "INVALID");

        assertEquals(new CarrierResolution(HttpCarrier.KT, true), reader.firstCarrier(phone));
        assertEquals(new CarrierResolution(HttpCarrier.SKT, false), reader.firstCarrier(phone));
        assertThrows(IllegalArgumentException.class, () -> reader.firstCarrier(phone));
        verifyNoInteractions(jdbc);
    }

    @Test
    void redisOutageDoesNotMasqueradeAsMissingPhoneMapping() {
        String phone = "01012345678";
        when(values.get(MessageReferenceCacheKeys.phoneCarrier(phone)))
                .thenThrow(new RedisConnectionFailureException("unavailable"));

        assertThrows(RedisConnectionFailureException.class, () -> reader.firstCarrier(phone));
        verifyNoInteractions(jdbc);
    }
}

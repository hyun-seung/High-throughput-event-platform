package messaging.pre.send.reference;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageReferenceCacheKeys;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Reads the CDC projection first; only a missing contract falls back to PostgreSQL. */
public final class PreSendReferenceReader {
    private final StringRedisTemplate redis;
    private final JdbcTemplate jdbc;
    private final JsonMapper mapper;

    public PreSendReferenceReader(StringRedisTemplate redis, JdbcTemplate jdbc, JsonMapper mapper) {
        this.redis = Objects.requireNonNull(redis);
        this.jdbc = Objects.requireNonNull(jdbc);
        this.mapper = Objects.requireNonNull(mapper);
    }

    public Optional<ClientMessageContract> findContract(long clientId) {
        if (clientId <= 0) throw new IllegalArgumentException("Invalid client ID");
        String cached = redis.opsForValue().get(MessageReferenceCacheKeys.contract(clientId));
        if (cached != null) return Optional.of(decodeContract(clientId, cached));
        return jdbc.query("SELECT client_id, enabled FROM delivery_results.client_message_contracts WHERE client_id = ?",
                (rs, row) -> new ClientMessageContract(rs.getLong("client_id"), rs.getBoolean("enabled")),
                clientId).stream().findFirst();
    }

    public CarrierResolution firstCarrier(String phoneNumber) {
        if (phoneNumber == null || !phoneNumber.matches("010[0-9]{8}")) {
            throw new IllegalArgumentException("Invalid recipient number");
        }
        String cached = redis.opsForValue().get(MessageReferenceCacheKeys.phoneCarrier(phoneNumber));
        if (cached == null) return new CarrierResolution(HttpCarrier.SKT, false);
        // An invalid value is an inconsistent projection, not an absent mapping.
        return new CarrierResolution(HttpCarrier.valueOf(cached), true);
    }

    @SuppressWarnings("unchecked")
    private ClientMessageContract decodeContract(long expectedClientId, String json) {
        Map<String, Object> row = mapper.readValue(json, Map.class);
        if (!(row.get("client_id") instanceof Number id) || id.longValue() != expectedClientId
                || !(row.get("enabled") instanceof Boolean enabled)) {
            throw new IllegalArgumentException("Invalid cached client contract");
        }
        return new ClientMessageContract(expectedClientId, enabled);
    }
}

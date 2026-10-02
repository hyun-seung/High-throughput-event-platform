package event.api.events;

import event.common.delivery.DeliveryPayloads;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.time.Duration;

@Component
public class EventDuplicateGuard {
    // The business deadline is at most seven hours; allow two more hours for a successful
    // completion marker and time for delayed cleanup. A lost cleanup cannot block forever.
    private static final Duration ACTIVE_KEY_TTL = Duration.ofHours(24);
    private static final RedisScript<Long> RELEASE = RedisScript.of("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
            """, Long.class);
    private final StringRedisTemplate redis;
    private final JsonMapper mapper;

    public EventDuplicateGuard(StringRedisTemplate redis, JsonMapper mapper) {
        this.redis = redis;
        this.mapper = mapper;
    }

    public Claim claim(long clientId, EventReceiveRequest request, String proposedExecutionId) {
        String key = key(clientId, request.eventId(), request.recipientNumber());
        String fingerprint = fingerprint(request);
        String value = proposedExecutionId + ":" + fingerprint;
        try {
            if (Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(key, value, ACTIVE_KEY_TTL))) {
                return new Claim(proposedExecutionId, key, value, true, true);
            }
            String existing = redis.opsForValue().get(key);
            if (existing == null) {
                throw new EventAdmissionException(HttpStatus.SERVICE_UNAVAILABLE, "Duplicate state changed; retry request");
            }
            int separator = existing.indexOf(':');
            if (separator < 1 || !existing.substring(separator + 1).equals(fingerprint)) {
                throw new EventAdmissionException(HttpStatus.CONFLICT, "eventId and recipientNumber already have different content");
            }
            return new Claim(existing.substring(0, separator), key, existing, false, true);
        } catch (RedisConnectionFailureException | QueryTimeoutException unavailable) {
            return new Claim(proposedExecutionId, key, value, true, false);
        }
    }

    public void release(Claim claim) {
        if (!claim.redisActive()) return;
        try {
            redis.execute(RELEASE, List.of(claim.key()), claim.value());
        } catch (RedisConnectionFailureException | QueryTimeoutException unavailable) {
            // A stale marker is safe to keep; the next duplicate must reconcile with ORIGIN.
        }
    }

    private String fingerprint(EventReceiveRequest request) {
        try {
            Map<String, Object> value = Map.of("eventId", request.eventId(), "recipientNumber", request.recipientNumber(),
                    "eventType", request.eventType().name(), "payload", DeliveryPayloads.canonicalize(request.payload()),
                    "fallbackAllowed", request.allowFallback());
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    mapper.writeValueAsBytes(value));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String key(long clientId, String eventId, String recipientNumber) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    (clientId + "\u0000" + eventId + "\u0000" + recipientNumber).getBytes(StandardCharsets.UTF_8));
            return "event:duplicate:{client:" + clientId + "}:" + HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public record Claim(String executionId, String key, String value, boolean owner, boolean redisActive) { }
}

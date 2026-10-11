package messaging.common.messages;

import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Objects;

/** Atomic, time-bounded claim for one HTTP or TCP provider call. */
public final class RedisSendAttemptGuard {
    private final StringRedisTemplate redis;

    public RedisSendAttemptGuard(StringRedisTemplate redis) {
        this.redis = Objects.requireNonNull(redis);
    }

    public boolean claimHttp(HttpSendCommand command, Duration ttl) {
        Objects.requireNonNull(command);
        // A claim identifies a provider call; retries and carrier moves have separate keys.
        String key = "message:http:attempt:" + command.request().clientMsgId() + ":" + command.carrier().name()
                + ":" + command.attemptId() + ":" + command.invocation();
        return claim(key, ttl);
    }

    public boolean claimTcp(String clientMsgId, String attemptId, Duration ttl) {
        if (clientMsgId == null || clientMsgId.isBlank() || attemptId == null || attemptId.isBlank()) {
            throw new IllegalArgumentException("TCP clientMsgId and attemptId are required");
        }
        return claim("message:tcp:attempt:" + clientMsgId + ":" + attemptId, ttl);
    }

    private boolean claim(String key, Duration ttl) {
        Objects.requireNonNull(ttl);
        if (ttl.isNegative() || ttl.isZero()) throw new IllegalArgumentException("Claim TTL must be positive");
        Boolean acquired = redis.opsForValue().setIfAbsent(key, "1", ttl);
        if (acquired == null) throw new IllegalStateException("Redis send claim returned no result");
        return acquired;
    }
}

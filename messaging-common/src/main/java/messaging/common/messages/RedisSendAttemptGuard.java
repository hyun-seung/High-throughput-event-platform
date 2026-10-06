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
        return claim(SendAttemptKeys.http(command), ttl);
    }

    public boolean claimTcp(String clientMsgId, String attemptId, Duration ttl) {
        return claim(SendAttemptKeys.tcp(clientMsgId, attemptId), ttl);
    }

    private boolean claim(String key, Duration ttl) {
        Objects.requireNonNull(ttl);
        if (ttl.isNegative() || ttl.isZero()) throw new IllegalArgumentException("Claim TTL must be positive");
        Boolean acquired = redis.opsForValue().setIfAbsent(key, "1", ttl);
        if (acquired == null) throw new IllegalStateException("Redis send claim returned no result");
        return acquired;
    }
}

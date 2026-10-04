package messaging.api.messages;

import messaging.common.messages.MessageCategory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.List;

/** Admission usage policy lives in Redis; no customer contract lookup occurs in the API. */
@Slf4j
@Component
public class MessageUsageLimiter {
    private static final ZoneId USAGE_ZONE = ZoneId.of("Asia/Seoul");
    private static final RedisScript<List> SCRIPT = RedisScript.of(
            new ClassPathResource("redis/message-usage.lua"), List.class);

    private final StringRedisTemplate redis;
    private final Clock clock;

    @Autowired
    public MessageUsageLimiter(StringRedisTemplate redis) {
        this(redis, Clock.systemUTC());
    }

    MessageUsageLimiter(StringRedisTemplate redis, Clock clock) {
        this.redis = redis;
        this.clock = clock;
    }

    /** Count every parsed request, including one later rejected as a customer duplicate. */
    public void charge(long clientId, MessageCategory type) {
        String prefix = "message:usage:{client:" + clientId + "}";
        String month = YearMonth.now(clock.withZone(USAGE_ZONE)).toString();
        List<String> keys = List.of(prefix + ":policy", prefix + ":10s",
                prefix + ":month:" + month + ":" + type.name());
        try {
            List<?> result = redis.execute(SCRIPT, keys, type.name());
            if (result == null || result.isEmpty()) throw new IllegalStateException("No message usage result");
            int status = Integer.parseInt(result.getFirst().toString());
            switch (status) {
                case 0 -> { return; }
                case 1 -> throw new MessageAdmissionException(HttpStatus.TOO_MANY_REQUESTS,
                        "10-second TPS limit exceeded");
                case 2 -> throw new MessageAdmissionException(HttpStatus.TOO_MANY_REQUESTS,
                        "Monthly message quota exceeded");
                case 3, 4 -> throw new MessageAdmissionException(HttpStatus.SERVICE_UNAVAILABLE,
                        "Message usage policy is not configured");
                default -> throw new IllegalStateException("Unknown message usage status: " + status);
            }
        } catch (RedisConnectionFailureException | QueryTimeoutException unavailable) {
            log.warn("Message usage check unavailable; admission continues: clientId={}", clientId);
        }
    }
}

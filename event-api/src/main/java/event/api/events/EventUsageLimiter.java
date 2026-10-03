package event.api.events;

import event.common.events.EventType;
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
public class EventUsageLimiter {
    private static final ZoneId USAGE_ZONE = ZoneId.of("Asia/Seoul");
    private static final RedisScript<List> SCRIPT = RedisScript.of(
            new ClassPathResource("redis/event-usage.lua"), List.class);

    private final StringRedisTemplate redis;
    private final Clock clock;

    @Autowired
    public EventUsageLimiter(StringRedisTemplate redis) {
        this(redis, Clock.systemUTC());
    }

    EventUsageLimiter(StringRedisTemplate redis, Clock clock) {
        this.redis = redis;
        this.clock = clock;
    }

    /** Count every parsed request, including one later rejected as a customer duplicate. */
    public void charge(long clientId, EventType type) {
        String prefix = "event:usage:{client:" + clientId + "}";
        String month = YearMonth.now(clock.withZone(USAGE_ZONE)).toString();
        List<String> keys = List.of(prefix + ":policy", prefix + ":10s",
                prefix + ":month:" + month + ":" + type.name());
        try {
            List<?> result = redis.execute(SCRIPT, keys, type.name());
            if (result == null || result.isEmpty()) throw new IllegalStateException("No event usage result");
            int status = Integer.parseInt(result.getFirst().toString());
            switch (status) {
                case 0 -> { return; }
                case 1 -> throw new EventAdmissionException(HttpStatus.TOO_MANY_REQUESTS,
                        "10-second TPS limit exceeded");
                case 2 -> throw new EventAdmissionException(HttpStatus.TOO_MANY_REQUESTS,
                        "Monthly event quota exceeded");
                case 3, 4 -> throw new EventAdmissionException(HttpStatus.SERVICE_UNAVAILABLE,
                        "Event usage policy is not configured");
                default -> throw new IllegalStateException("Unknown event usage status: " + status);
            }
        } catch (RedisConnectionFailureException | QueryTimeoutException unavailable) {
            log.warn("Event usage check unavailable; admission continues: clientId={}", clientId);
        }
    }
}

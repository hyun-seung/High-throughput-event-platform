package event.api.events;

import event.common.events.EventType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;

import java.time.Clock;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@EnabledIfEnvironmentVariable(named = "REDIS_TEST_PORT", matches = ".+")
class EventUsageLimiterIntegrationTest {
    @Test
    void chargesEachEventTypeAndRejectsTheThirdNotification() {
        var factory = new LettuceConnectionFactory("localhost", Integer.parseInt(System.getenv("REDIS_TEST_PORT")));
        factory.afterPropertiesSet();
        factory.start();
        var redis = new StringRedisTemplate(factory);
        long clientId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
        String prefix = "event:usage:{client:" + clientId + "}";
        String month = YearMonth.now(Clock.system(ZoneId.of("Asia/Seoul"))).toString();
        String policy = prefix + ":policy";
        String bucket = prefix + ":10s";
        String noti = prefix + ":month:" + month + ":NOTI";
        String general = prefix + ":month:" + month + ":GENERAL";
        try {
            redis.opsForHash().putAll(policy, Map.of("tpsLimit", "1000", "quotaNOTI", "2",
                    "quotaGENERAL", "2"));
            var limiter = new EventUsageLimiter(redis);
            limiter.charge(clientId, EventType.NOTI);
            limiter.charge(clientId, EventType.NOTI);
            assertEquals(HttpStatus.TOO_MANY_REQUESTS,
                    assertThrows(EventAdmissionException.class,
                            () -> limiter.charge(clientId, EventType.NOTI)).status());
            limiter.charge(clientId, EventType.GENERAL);
            assertEquals("3", redis.opsForValue().get(noti));
            assertEquals("1", redis.opsForValue().get(general));
            assertEquals(2L, redis.opsForHash().size(bucket));
        } finally {
            redis.delete(policy);
            redis.delete(bucket);
            redis.delete(noti);
            redis.delete(general);
            factory.destroy();
        }
    }
}

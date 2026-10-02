package event.api.events;

import event.common.events.EventType;
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

@Component
public class EventUsageLimiter {
    private static final ZoneId CONTRACT_ZONE = ZoneId.of("Asia/Seoul");
    private final StringRedisTemplate redis;
    private final ClientEventContractRepository contracts;
    private final Clock clock;
    private final RedisScript<List> script = RedisScript.of(new ClassPathResource("redis/event-usage.lua"), List.class);

    public EventUsageLimiter(StringRedisTemplate redis, ClientEventContractRepository contracts) {
        this(redis, contracts, Clock.systemUTC());
    }

    EventUsageLimiter(StringRedisTemplate redis, ClientEventContractRepository contracts, Clock clock) {
        this.redis = redis;
        this.contracts = contracts;
        this.clock = clock;
    }

    /** Charges every parsed request, including one later rejected as a customer duplicate. */
    public void charge(long clientId, EventType type) {
        ClientEventContractRepository.Contract contract = contracts.get(clientId);
        String month = YearMonth.now(clock.withZone(CONTRACT_ZONE)).toString();
        List<String> keys = List.of("event:usage:{client:" + clientId + "}:10s",
                "event:usage:{client:" + clientId + "}:month:" + month + ":" + type.name());
        try {
            List<?> result = redis.execute(script, keys, String.valueOf(contract.tpsLimit() * 10L),
                    String.valueOf(contract.quota(type)));
            if (result == null || result.isEmpty()) throw new IllegalStateException("No usage result");
            int status = Integer.parseInt(result.get(0).toString());
            if (status == 1) throw new EventAdmissionException(HttpStatus.TOO_MANY_REQUESTS, "10-second TPS limit exceeded");
            if (status == 2) throw new EventAdmissionException(HttpStatus.TOO_MANY_REQUESTS, "Monthly event quota exceeded");
        } catch (RedisConnectionFailureException | QueryTimeoutException unavailable) {
            // The contract remains required; only the Redis usage limit is temporarily bypassed.
        }
    }
}

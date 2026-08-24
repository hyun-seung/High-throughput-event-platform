package event.event.api.requestcontrol.redis;

import event.event.api.requestcontrol.RequestLimiter;
import event.event.api.requestcontrol.result.RequestLimitResult;
import event.event.api.requestcontrol.result.RequestLimitStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.YearMonth;
import java.util.List;
import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class RedisRequestLimiter implements RequestLimiter {

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<List> requestLimitScript;

    @Override
    public RequestLimitResult tryAcquire(Long userId) {
        Objects.requireNonNull(userId, "userId must not be null.");

        List<String> keys = List.of(
                RequestControlRedisKey.policy(userId),
                RequestControlRedisKey.bucket(userId),
                RequestControlRedisKey.quota(userId, YearMonth.now())
        );

        try {
            List<?> result = redisTemplate.execute(requestLimitScript, keys);
            return convertResult(userId, result);
        } catch (RedisConnectionFailureException | QueryTimeoutException e) {
            log.error("Redis unavailable. Request control bypassed. userId={}, cause={}", userId, e.getMessage());
            return RequestLimitResult.redisUnavailableBypass();
        }
    }

    private RequestLimitResult convertResult(Long userId, List<?> result) {
        if (Objects.isNull(result) || result.size() < RequestLimitResultField.size()) {
            log.error("Invalid request limit script result. userId={}, result={}", userId, result);
            throw new IllegalStateException("Invalid request limit script result.");
        }

        long statusCode = toLong(result.get(RequestLimitResultField.STATUS.getIndex()));
        long remainingTokens = toLong(result.get(RequestLimitResultField.REMAINING_TOKENS.getIndex()));
        long monthlyUsage = toLong(result.get(RequestLimitResultField.MONTHLY_USAGE.getIndex()));
        long monthlyLimit = toLong(result.get(RequestLimitResultField.MONTHLY_LIMIT.getIndex()));

        RequestLimitStatus status = RequestLimitStatus.fromScriptCode(statusCode);

        if (status != RequestLimitStatus.ALLOWED) {
            log.debug("Request limited. userId={}, status={}, remainingTokens={}, monthlyUsage={}, monthlyLimit={}",
                    userId, status, remainingTokens, monthlyUsage, monthlyLimit);
        }

        return new RequestLimitResult(status, remainingTokens, monthlyUsage, monthlyLimit);
    }

    private long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }

        return Long.parseLong(String.valueOf(value));
    }
}
package event.api.requestcontrol.redis;

import event.api.requestcontrol.RequestLimiter;
import event.api.requestcontrol.result.RequestLimitResult;
import event.api.requestcontrol.result.RequestLimitStatus;
import event.common.recovery.FailureBackoff;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
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
public class RedisRequestLimiter implements RequestLimiter {

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<List> requestLimitScript;
    private final FailureBackoff backoff;
    private final MeterRegistry metrics;
    public RedisRequestLimiter(StringRedisTemplate redisTemplate, RedisScript<List> requestLimitScript,
            @Qualifier("redisRecoveryBackoff") FailureBackoff backoff, MeterRegistry metrics) {
        this.redisTemplate = redisTemplate; this.requestLimitScript = requestLimitScript; this.backoff = backoff; this.metrics = metrics;
    }

    @Override
    public RequestLimitResult tryAcquire(Long userId) {
        Objects.requireNonNull(userId, "userId must not be null.");

        List<String> keys = List.of(
                RequestControlRedisKey.policy(userId),
                RequestControlRedisKey.bucket(userId),
                RequestControlRedisKey.quota(userId, YearMonth.now())
        );

        var ticket = backoff.acquire();
        if (ticket == null) {
            metrics.counter("delivery.redis.calls.skipped", "operation", "request_limit").increment();
            return RequestLimitResult.redisUnavailableBypass();
        }
        try {
            List<?> result = redisTemplate.execute(requestLimitScript, keys);
            var converted = convertResult(userId, result); backoff.succeeded(ticket); return converted;
        } catch (RedisConnectionFailureException | QueryTimeoutException e) {
            if (backoff.failed(ticket)) log.warn("Redis unavailable; request control bypassed until recovery probe. failure={}", e.getClass().getSimpleName());
            return RequestLimitResult.redisUnavailableBypass();
        } catch (RuntimeException invalid) {
            backoff.succeeded(ticket); throw invalid;
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

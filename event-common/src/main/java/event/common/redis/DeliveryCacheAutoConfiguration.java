package event.common.redis;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.time.Duration;
import event.common.recovery.FailureBackoff;
import org.springframework.beans.factory.annotation.Qualifier;

@AutoConfiguration(afterName = "org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration")
public class DeliveryCacheAutoConfiguration {
    @Bean("redisRecoveryBackoff") @ConditionalOnMissingBean(name = "redisRecoveryBackoff")
    FailureBackoff redisRecoveryBackoff(ObjectProvider<MeterRegistry> metrics,
            @Value("${delivery.redis-recovery.initial-delay:1s}") Duration initial,
            @Value("${delivery.redis-recovery.max-delay:30s}") Duration maximum) {
        var backoff = new FailureBackoff(initial, maximum);
        var registry = metrics.getIfAvailable();
        if (registry != null) registry.gauge("delivery.redis.recovery.delay", backoff, FailureBackoff::remainingSeconds);
        return backoff;
    }
    @Bean @ConditionalOnMissingBean
    DeliveryCache deliveryCache(ObjectProvider<StringRedisTemplate> redis, ObjectProvider<MeterRegistry> metrics,
            @Value("${delivery.completed-retention:24h}") Duration retention,
            @Qualifier("redisRecoveryBackoff") FailureBackoff backoff) {
        return new DeliveryCache(redis.getIfAvailable(), metrics.getIfAvailable(), retention, backoff);
    }
}

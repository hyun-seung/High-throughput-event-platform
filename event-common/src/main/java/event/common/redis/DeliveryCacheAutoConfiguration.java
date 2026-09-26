package event.common.redis;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.resource.Delay;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.data.redis.autoconfigure.ClientResourcesBuilderCustomizer;
import org.springframework.boot.data.redis.autoconfigure.LettuceClientOptionsBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import event.common.recovery.FailureBackoff;
import org.springframework.beans.factory.annotation.Qualifier;

@AutoConfiguration(afterName = "org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration")
public class DeliveryCacheAutoConfiguration {
    @Bean
    LettuceClientOptionsBuilderCustomizer redisClientOptions(
            @Value("${delivery.redis-recovery.request-queue-size:1024}") int queueSize) {
        if (queueSize < 1) throw new IllegalArgumentException("Positive Redis request queue size required");
        return options -> options.autoReconnect(true)
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .requestQueueSize(queueSize);
    }

    @Bean
    ClientResourcesBuilderCustomizer redisReconnectDelay() {
        return resources -> resources.reconnectDelay(
                Delay.equalJitter(Duration.ofMillis(100), Duration.ofSeconds(30), 100, TimeUnit.MILLISECONDS));
    }

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

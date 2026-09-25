package event.common.redis;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.time.Duration;

@AutoConfiguration(afterName = "org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration")
public class DeliveryCacheAutoConfiguration {
    @Bean @ConditionalOnMissingBean
    DeliveryCache deliveryCache(ObjectProvider<StringRedisTemplate> redis, ObjectProvider<MeterRegistry> metrics,
            @Value("${delivery.completed-retention:24h}") Duration retention) {
        return new DeliveryCache(redis.getIfAvailable(), metrics.getIfAvailable(), retention);
    }
}

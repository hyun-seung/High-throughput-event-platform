package event.common.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
public class DeliveryMetricsAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public DeliveryMetrics deliveryMetrics(MeterRegistry registry) {
        return new DeliveryMetrics(registry);
    }
}

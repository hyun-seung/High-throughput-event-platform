package event.delivery.result.cleanup;

import event.common.lifecycle.DeliveryCompactor;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "cleanup.enabled", havingValue = "true")
public class CleanupConfiguration {
    @Bean org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler cleanupTaskScheduler() {
        var scheduler = new org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1); scheduler.setThreadNamePrefix("delivery-cleanup-"); return scheduler;
    }
    @Bean CleanupRepository cleanupRepository(JdbcTemplate jdbc, PlatformTransactionManager manager) { return new CleanupRepository(jdbc, manager); }
    @Bean DeliveryCompactor deliveryCompactor(DynamoDbClient db, event.common.redis.DeliveryCache cache) { return new DeliveryCompactor(db, cache); }
    @Bean CleanupWorker cleanupWorker(CleanupRepository repository, DeliveryCompactor compactor, MeterRegistry meters) {
        return new CleanupWorker(repository, compactor, meters);
    }
}

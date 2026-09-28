package event.delivery.result.operations;

import event.common.lifecycle.ManualResolution;
import event.common.recovery.FailureBackoff;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Clock;
import java.time.Duration;

@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "resolution.recovery.enabled", havingValue = "true")
public class ResolutionRecoveryConfiguration {
    @Bean ThreadPoolTaskScheduler resolutionRecoveryScheduler() {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1); scheduler.setThreadNamePrefix("resolution-recovery-");
        return scheduler;
    }

    @Bean ResolutionOperations resolutionOperations(JdbcTemplate sql, PlatformTransactionManager manager, DynamoDbClient db) {
        return new ResolutionOperations(sql, manager, new ManualResolution(db), Clock.systemUTC());
    }

    @Bean ResolutionRecoveryWorker resolutionRecoveryWorker(ResolutionOperations operations, MeterRegistry meters,
            @Value("${resolution.recovery.batch-size:5}") int batchSize,
            @Value("${resolution.recovery.failure-initial-delay:1s}") Duration initial,
            @Value("${resolution.recovery.failure-max-delay:30s}") Duration maximum) {
        return new ResolutionRecoveryWorker(operations, meters, new FailureBackoff(initial, maximum), batchSize);
    }
}

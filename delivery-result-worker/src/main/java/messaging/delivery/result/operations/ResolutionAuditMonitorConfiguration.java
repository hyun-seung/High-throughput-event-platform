package messaging.delivery.result.operations;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.time.Clock;

@Configuration
@EnableScheduling
public class ResolutionAuditMonitorConfiguration {
    @Bean ThreadPoolTaskScheduler resolutionAuditMonitorScheduler() {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1); scheduler.setThreadNamePrefix("resolution-audit-monitor-");
        return scheduler;
    }

    @Bean ResolutionAuditMonitor resolutionAuditMonitor(JdbcTemplate sql, MeterRegistry meters) {
        return new ResolutionAuditMonitor(sql, meters, Clock.systemUTC());
    }
}

package event.delivery.result.notification;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(NotificationProperties.class)
@ConditionalOnProperty(name = "notification.enabled", havingValue = "true")
public class NotificationConfiguration {
    @Bean NotificationRepository notificationRepository(JdbcTemplate jdbc, PlatformTransactionManager manager) {
        return new NotificationRepository(jdbc, manager);
    }
    @Bean(destroyMethod = "close") CustomerNotificationClient customerNotificationClient(NotificationProperties settings) {
        return new CustomerNotificationClient(settings.httpTimeout());
    }
    @Bean NotificationService notificationService(NotificationRepository repository, CustomerNotificationClient client, NotificationProperties settings, MeterRegistry meters) {
        return new NotificationService(repository, client, settings, meters);
    }
    @Bean(destroyMethod = "close") NotificationScheduler notificationScheduler(NotificationService service, NotificationProperties settings, MeterRegistry meters) {
        return new NotificationScheduler(service, settings, meters);
    }
}

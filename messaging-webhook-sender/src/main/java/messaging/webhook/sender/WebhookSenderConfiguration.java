package messaging.webhook.sender;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
@EnableConfigurationProperties(WebhookSenderProperties.class)
public class WebhookSenderConfiguration {
    @Bean DefaultErrorHandler webhookSendErrorHandler() {
        var handler = new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setClassifications(Map.of(Exception.class, true), true);
        return handler;
    }
}

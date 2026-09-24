package event.delivery.dispatch.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
public class DispatchFailureConfiguration {

    @Bean
    public DefaultErrorHandler dispatchErrorHandler(
            @Value("${dispatch.redelivery-backoff-ms:1000}") long redeliveryBackoffMillis
    ) {
        if (redeliveryBackoffMillis < 0) {
            throw new IllegalArgumentException("dispatch.redelivery-backoff-ms must be non-negative");
        }
        var handler = new DefaultErrorHandler((record, failure) -> {
            throw new KafkaException("Dispatch recovery has not been durably handed off", failure);
        }, new FixedBackOff(redeliveryBackoffMillis, FixedBackOff.UNLIMITED_ATTEMPTS));
        // Until Dispatch has a durable DLT path, even malformed records must not be logged and dropped.
        handler.setClassifications(Map.of(), true);
        return handler;
    }
}

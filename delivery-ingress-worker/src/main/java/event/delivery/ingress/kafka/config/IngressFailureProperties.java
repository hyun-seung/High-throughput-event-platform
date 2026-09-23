package event.delivery.ingress.kafka.config;

import event.common.delivery.DeliveryTopics;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Infrastructure redelivery settings, independent of external provider retries. */
@ConfigurationProperties(prefix = "delivery.ingress.failure")
public record IngressFailureProperties(Duration retryInterval, Long maxRetries, String dltTopic) {

    public IngressFailureProperties {
        retryInterval = retryInterval == null ? Duration.ofSeconds(1) : retryInterval;
        maxRetries = maxRetries == null ? 2L : maxRetries;
        dltTopic = dltTopic == null ? DeliveryTopics.DELIVERY_REQUESTED_DLT : dltTopic;
        if (retryInterval.isNegative() || maxRetries < 0 || dltTopic.isBlank()) {
            throw new IllegalArgumentException("Ingress retry interval/count must be non-negative and DLT topic non-blank");
        }
    }
}

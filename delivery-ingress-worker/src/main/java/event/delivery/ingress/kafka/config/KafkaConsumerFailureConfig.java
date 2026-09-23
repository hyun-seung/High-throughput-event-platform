package event.delivery.ingress.kafka.config;

import event.delivery.ingress.repository.IdempotencyConflictException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
@EnableConfigurationProperties(IngressFailureProperties.class)
public class KafkaConsumerFailureConfig {

    @Bean
    public DeadLetterPublishingRecoverer ingressDeadLetterRecoverer(
            KafkaTemplate<Object, Object> dltKafkaTemplate, IngressFailureProperties properties
    ) {
        var recoverer = new DeadLetterPublishingRecoverer(dltKafkaTemplate,
                (record, failure) -> new TopicPartition(properties.dltTopic(), record.partition()));
        // Never treat an asynchronous DLT send failure or timeout as recovery.
        recoverer.setFailIfSendResultIsError(true);
        // Keep the source partition. Missing DLT partitions must fail, not silently reroute.
        recoverer.setVerifyPartition(false);
        return recoverer;
    }

    @Bean
    public DefaultErrorHandler ingressErrorHandler(
            DeadLetterPublishingRecoverer ingressDeadLetterRecoverer, IngressFailureProperties properties
    ) {
        var handler = new DefaultErrorHandler(ingressDeadLetterRecoverer,
                new FixedBackOff(properties.retryInterval().toMillis(), properties.maxRetries()));
        handler.addNotRetryableExceptions(IdempotencyConflictException.class);
        // RECORD ack mode commits only after successful processing or durable recovery.
        handler.setAckAfterHandle(true);
        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception failure, int attempt) {
                log.debug("Ingress delivery failed. topic={}, partition={}, offset={}, attempt={}",
                        record.topic(), record.partition(), record.offset(), attempt);
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception failure) {
                log.warn("Ingress record stored in DLT. topic={}, partition={}, offset={}, dlt={}",
                        record.topic(), record.partition(), record.offset(), properties.dltTopic());
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
                log.error("DLT storage unconfirmed; retaining source offset. topic={}, partition={}, offset={}",
                        record.topic(), record.partition(), record.offset());
            }
        });
        return handler;
    }
}

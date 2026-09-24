package event.delivery.dispatch.receipt;

import event.common.receipt.ReceiptEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

@Configuration
public class ReceiptConsumerConfiguration {
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ReceiptEvent> receiptListenerContainerFactory(
            KafkaProperties kafka, DefaultErrorHandler dispatchErrorHandler,
            @Value("${dispatch.receipts.concurrency:3}") int concurrency) {
        if (concurrency < 1) throw new IllegalArgumentException("Receipt concurrency must be positive");
        var properties = kafka.buildConsumerProperties();
        properties.put("enable.auto.commit", false);
        properties.put("spring.json.value.default.type", ReceiptEvent.class.getName());
        properties.put("spring.json.trusted.packages", "event.common.receipt");
        properties.put("spring.json.use.type.headers", false);
        var factory = new ConcurrentKafkaListenerContainerFactory<String, ReceiptEvent>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(properties));
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(dispatchErrorHandler);
        factory.setAutoStartup(kafka.getListener().isAutoStartup());
        return factory;
    }
}

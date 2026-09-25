package event.delivery.result;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
public class ResultConsumerConfiguration {
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> finalizedListenerContainerFactory(
            KafkaProperties kafka, @Value("${result.concurrency:3}") int concurrency,
            @Value("${result.redelivery-backoff-ms:1000}") long backoff) {
        if (concurrency < 1 || backoff < 0) throw new IllegalArgumentException("Invalid result consumer settings");
        var props = kafka.buildConsumerProperties();
        props.put("enable.auto.commit", false);
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", ByteArrayDeserializer.class);
        var factory = new ConcurrentKafkaListenerContainerFactory<String, byte[]>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setMissingTopicsFatal(true);
        var handler = new DefaultErrorHandler((record, error) -> {
            throw new KafkaException("Final result has no durable recovery handoff", error);
        }, new FixedBackOff(backoff, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setClassifications(Map.of(), true);
        factory.setCommonErrorHandler(handler);
        factory.setAutoStartup(kafka.getListener().isAutoStartup());
        return factory;
    }
}

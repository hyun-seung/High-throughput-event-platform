package event.delivery.dispatch.retry;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.*;
import tools.jackson.databind.json.JsonMapper;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@Configuration
public class RetryConfiguration {
    public static final String TOPIC = "delivery.retry.v1";
    @Bean NewTopic retryTopic(@Value("${dispatch.retry.partitions:3}") int partitions,
            @Value("${dispatch.retry.replicas:1}") int replicas,
            @Value("${dispatch.retry.min-insync-replicas:1}") int minIsr) {
        if (partitions < 1 || minIsr < 1 || minIsr > replicas) throw new IllegalArgumentException("Invalid retry replication");
        return TopicBuilder.name(TOPIC).partitions(partitions).replicas(replicas)
                .config("min.insync.replicas", Integer.toString(minIsr)).config("retention.ms", "604800000").build();
    }
    @Bean(destroyMethod = "close") RetryTransport retryPublisher(KafkaProperties kafka, JsonMapper mapper) {
        return new RetryTransport(kafka, mapper);
    }
    public static class RetryTransport implements RetryPublisher, AutoCloseable {
        private final KafkaProducer<String, byte[]> producer;
        private final JsonMapper mapper;
        private final String topic;
        public RetryTransport(KafkaProperties kafka, JsonMapper mapper) { this(kafka, mapper, TOPIC); }
        public RetryTransport(KafkaProperties kafka, JsonMapper mapper, String topic) {
            this.mapper = mapper; this.topic = topic;
            var config = new HashMap<String, Object>(kafka.buildProducerProperties());
            config.put("key.serializer", StringSerializer.class); config.put("value.serializer", ByteArraySerializer.class);
            config.put("acks", "all"); config.put("enable.idempotence", true);
            config.put("max.block.ms", 5000); config.put("request.timeout.ms", 5000); config.put("delivery.timeout.ms", 10000);
            producer = new KafkaProducer<>(config);
        }
        public void publish(RetryCommand command) {
            try { producer.send(new ProducerRecord<>(topic, command.event().deliveryId(), mapper.writeValueAsBytes(command))).get(12, TimeUnit.SECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException("Retry publish interrupted", interrupted); }
            catch (Exception failure) { throw new IllegalStateException("Retry publication not confirmed", failure); }
        }
        public void close() { producer.close(java.time.Duration.ofSeconds(5)); }
    }
    @Bean ConcurrentKafkaListenerContainerFactory<String, RetryCommand> retryListenerContainerFactory(
            KafkaProperties kafka, DefaultErrorHandler dispatchErrorHandler) {
        var config = kafka.buildConsumerProperties();
        config.put("enable.auto.commit", false);
        config.put("spring.json.value.default.type", RetryCommand.class.getName());
        config.put("spring.json.trusted.packages", "event.delivery.dispatch.retry");
        config.put("spring.json.use.type.headers", false);
        var factory = new ConcurrentKafkaListenerContainerFactory<String, RetryCommand>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(dispatchErrorHandler);
        factory.setAutoStartup(kafka.getListener().isAutoStartup());
        return factory;
    }
}

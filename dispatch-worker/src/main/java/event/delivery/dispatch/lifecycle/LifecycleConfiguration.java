package event.delivery.dispatch.lifecycle;

import event.common.lifecycle.DeliveryFinalized;
import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.receipt.ReceiptResultRepository;
import event.delivery.dispatch.service.DispatchService;
import event.delivery.dispatch.service.SecondaryDispatchService;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "dispatch.lifecycle.enabled", havingValue = "true")
public class LifecycleConfiguration {
    @Bean LifecycleRepository lifecycleRepository(DynamoDbClient db, JsonMapper mapper) {
        var repository = new LifecycleRepository(db, mapper); repository.requireIndex(); return repository;
    }
    @Bean NewTopic finalizedTopic(@Value("${dispatch.lifecycle.topic:delivery.finalized.v1}") String topic,
                                  @Value("${dispatch.lifecycle.partitions:3}") int partitions,
                                  @Value("${dispatch.lifecycle.replicas:1}") int replicas,
                                  @Value("${dispatch.lifecycle.min-insync-replicas:1}") int minIsr) {
        if (partitions < 1 || replicas < 1 || minIsr < 1 || minIsr > replicas) throw new IllegalArgumentException("Invalid finalized topic replication");
        return TopicBuilder.name(topic).partitions(partitions).replicas(replicas)
                .config("min.insync.replicas", Integer.toString(minIsr)).config("retention.ms", "604800000").build();
    }
    @Bean(destroyMethod = "close") KafkaProducer<String, byte[]> finalizedProducer(KafkaProperties kafka) {
        var config = new HashMap<String, Object>(kafka.buildProducerProperties());
        config.put("key.serializer", StringSerializer.class); config.put("value.serializer", ByteArraySerializer.class);
        config.put("acks", "all"); config.put("enable.idempotence", true); config.put("compression.type", "zstd");
        config.put("linger.ms", 5); config.put("max.block.ms", 5000); config.put("request.timeout.ms", 5000);
        config.put("delivery.timeout.ms", 10000);
        return new KafkaProducer<>(config);
    }
    @Bean FinalizedPublisher finalizedPublisher(KafkaProducer<String, byte[]> producer, JsonMapper mapper,
                                                @Value("${dispatch.lifecycle.topic:delivery.finalized.v1}") String topic) {
        return result -> {
            try { producer.send(new ProducerRecord<>(topic, result.deliveryId(), mapper.writeValueAsBytes(result))).get(12, TimeUnit.SECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException("Finalized publish interrupted", interrupted); }
            catch (Exception failure) { throw new IllegalStateException("Finalized acknowledgement unconfirmed", failure); }
        };
    }
    @Bean LifecycleService lifecycleService(LifecycleRepository repository, ReceiptResultRepository sources,
            DispatchService dispatch, SecondaryDispatchService secondary, DispatchProperties config,
            FinalizedPublisher publisher, Clock clock, MeterRegistry metrics, event.common.redis.DeliveryCache cache) {
        var service = new LifecycleService(repository, sources, dispatch, secondary, config, publisher, clock, metrics);
        service.cache(cache); return service;
    }
    @Bean LifecycleScheduler lifecycleScheduler(LifecycleRepository repository, LifecycleService service, Clock clock,
            MeterRegistry metrics, @Value("${dispatch.lifecycle.page-size:100}") int pageSize,
            @Value("${dispatch.lifecycle.concurrency:4}") int concurrency,
            event.common.redis.DeliveryCache cache, @Value("${dispatch.lifecycle.recovery-poll-ms:600000}") long recoveryMs,
            @Value("${dispatch.lifecycle.failure-initial-delay:1s}") java.time.Duration initial,
            @Value("${dispatch.lifecycle.failure-max-delay:30s}") java.time.Duration maximum) {
        var scheduler = new LifecycleScheduler(repository, service, clock, metrics, pageSize, concurrency,
                new event.common.recovery.FailureBackoff(initial, maximum), new event.common.recovery.FailureBackoff(initial, maximum));
        scheduler.cache(cache, recoveryMs); return scheduler;
    }
}

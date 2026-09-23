package event.delivery.ingress.kafka.config;

import event.common.delivery.DeliveryEvent;
import event.delivery.ingress.repository.IdempotencyConflictException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import tools.jackson.databind.json.JsonMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Real local broker, production recoverer/serializers, isolated topics and consumer groups. */
@EnabledIfEnvironmentVariable(named = "KAFKA_TEST_BOOTSTRAP_SERVERS", matches = ".+")
class IngressDltKafkaTest {

    private static String bootstrap;
    private static Admin admin;
    private String sourceTopic;
    private String dltTopic;
    private String group;
    private ProducerFactory<Object, Object> factory;
    private KafkaTemplate<Object, Object> template;
    private KafkaProducer<byte[], byte[]> input;
    private KafkaMessageListenerContainer<String, DeliveryEvent> container;
    private DefaultErrorHandler handler;

    @BeforeAll
    static void connect() {
        bootstrap = System.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS");
        for (String address : bootstrap.split(",")) {
            if (!address.trim().matches("(localhost|127\\.0\\.0\\.1):[0-9]+")) {
                throw new IllegalArgumentException("KAFKA_TEST_BOOTSTRAP_SERVERS must point to a local broker");
            }
        }
        admin = Admin.create(Map.of("bootstrap.servers", bootstrap,
                "request.timeout.ms", 5000, "default.api.timeout.ms", 10000));
    }

    @BeforeEach
    void setUp() throws Exception {
        String id = "test.ingress-dlt." + UUID.randomUUID();
        sourceTopic = id + ".source";
        dltTopic = id + ".dlt";
        group = id + ".worker";
        admin.createTopics(List.of(new NewTopic(sourceTopic, 2, (short) 1),
                new NewTopic(dltTopic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);

        var kafka = new KafkaProperties();
        kafka.setBootstrapServers(List.of(bootstrap.split(",")));
        kafka.getProducer().setAcks("all");
        kafka.getProducer().getProperties().putAll(Map.of(
                "enable.idempotence", "true", "spring.json.add.type.headers", "false",
                "max.block.ms", "500", "request.timeout.ms", "500", "delivery.timeout.ms", "1000"));
        factory = new KafkaProducerConfig().dltProducerFactory(kafka);
        template = new KafkaProducerConfig().dltKafkaTemplate(factory);
        var failure = new IngressFailureProperties(Duration.ofMillis(20), 0L, dltTopic);
        var config = new KafkaConsumerFailureConfig();
        handler = config.ingressErrorHandler(config.ingressDeadLetterRecoverer(template, failure), failure);
        input = new KafkaProducer<>(Map.of("bootstrap.servers", bootstrap, "acks", "all"),
                new ByteArraySerializer(), new ByteArraySerializer());
    }

    @AfterEach
    void cleanUp() throws Exception {
        if (container != null) container.stop();
        if (input != null) input.close(Duration.ofSeconds(5));
        if (template != null) template.destroy();
        if (factory != null) factory.reset();
        if (sourceTopic != null) {
            // Only UUID-named test resources are touched; no business topic/group resets.
            admin.deleteTopics(List.of(sourceTopic, dltTopic)).all().get(10, TimeUnit.SECONDS);
            admin.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
        }
    }

    @AfterAll
    static void close() {
        if (admin != null) admin.close(Duration.ofSeconds(5));
    }

    @Test
    void conflictIsStoredInDltAndFollowingRequestContinues() throws Exception {
        var failures = new AtomicInteger();
        var healthy = new CountDownLatch(1);
        start(0, record -> {
            if ("conflict".equals(record.key())) {
                failures.incrementAndGet();
                throw new IdempotencyConflictException(record.value().deliveryId());
            }
            healthy.countDown();
        });
        byte[] body = body("delivery-conflict");
        send(0, "conflict", body);
        send(0, "healthy", body("delivery-healthy"));

        ConsumerRecord<byte[], byte[]> dlt = readDlt(0);
        assertArrayEquals(body, dlt.value());
        assertArrayEquals(bytes("conflict"), dlt.key());
        assertEquals(sourceTopic, new String(dlt.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC).value(), StandardCharsets.UTF_8));
        assertEquals(0L, ByteBuffer.wrap(dlt.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET).value()).getLong());
        assertTrue(healthy.await(10, TimeUnit.SECONDS));
        awaitCommit(0, 2);
        assertEquals(1, failures.get());
    }

    @Test
    void malformedJsonIsPreservedAsBytesAndNeverPassedToListener() throws Exception {
        var calls = new AtomicInteger();
        start(0, record -> calls.incrementAndGet());
        byte[] malformed = bytes("{not-json");
        send(0, "broken", malformed);
        send(0, "healthy", body("delivery-healthy"));

        ConsumerRecord<byte[], byte[]> dlt = readDlt(0);
        assertArrayEquals(malformed, dlt.value());
        assertArrayEquals(bytes("broken"), dlt.key());
        assertNotNull(dlt.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN));
        awaitCommit(0, 2);
        assertEquals(1, calls.get());
    }

    @Test
    void unavailableDltPartitionRetainsSourceOffsetAndRecoversAfterRepair() throws Exception {
        var recoveryFailed = new CountDownLatch(1);
        var following = new CountDownLatch(1);
        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception exception, int attempt) { }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
                recoveryFailed.countDown();
            }
        });
        start(1, record -> {
            if ("failed".equals(record.key())) throw new IllegalStateException("downstream unavailable");
            if ("following".equals(record.key())) following.countDown();
        });
        send(1, "baseline", body("delivery-baseline"));
        awaitCommit(1, 1);
        send(1, "failed", body("delivery-failed"));
        send(1, "following", body("delivery-following"));

        assertTrue(recoveryFailed.await(15, TimeUnit.SECONDS), "DLT partition 1 does not exist yet");
        assertEquals(1L, committed(1), "Failed DLT storage must not advance past the failed source offset");
        assertEquals(1L, following.getCount(), "Later records cannot bypass unhandled input");

        admin.createPartitions(Map.of(dltTopic, NewPartitions.increaseTo(2))).all().get(10, TimeUnit.SECONDS);

        assertArrayEquals(bytes("failed"), readDlt(1).key());
        assertTrue(following.await(10, TimeUnit.SECONDS));
        awaitCommit(1, 3);
    }

    private void start(int partition, MessageListener<String, DeliveryEvent> listener) {
        var consumerFactory = new DefaultKafkaConsumerFactory<>(Map.of(
                "bootstrap.servers", bootstrap, "group.id", group,
                "enable.auto.commit", false, "auto.offset.reset", "earliest"),
                new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(new JacksonJsonDeserializer<>(DeliveryEvent.class, false)));
        var properties = new ContainerProperties(new TopicPartitionOffset(sourceTopic, partition));
        properties.setGroupId(group);
        properties.setAckMode(ContainerProperties.AckMode.RECORD);
        properties.setPollTimeout(100);
        properties.setShutdownTimeout(5000);
        properties.setMessageListener(listener);
        container = new KafkaMessageListenerContainer<>(consumerFactory, properties);
        container.setCommonErrorHandler(handler);
        container.start();
    }

    private byte[] body(String deliveryId) {
        return JsonMapper.builder().build().writeValueAsBytes(DeliveryEvent.requested(
                deliveryId, 999L, "EMAIL", Map.of("body", "test"), Instant.parse("2026-09-23T00:00:00Z")));
    }

    private void send(int partition, String key, byte[] body) throws Exception {
        input.send(new ProducerRecord<>(sourceTopic, partition, bytes(key), body)).get(10, TimeUnit.SECONDS);
    }

    private ConsumerRecord<byte[], byte[]> readDlt(int partition) {
        try (var consumer = new KafkaConsumer<>(Map.of("bootstrap.servers", bootstrap, "enable.auto.commit", false),
                new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            var topicPartition = new TopicPartition(dltTopic, partition);
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, 0);
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) return records.iterator().next();
            }
            throw new AssertionError("No DLT record for " + topicPartition);
        }
    }

    private long committed(int partition) throws Exception {
        var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        var offset = offsets.get(new TopicPartition(sourceTopic, partition));
        return offset == null ? -1 : offset.offset();
    }

    private void awaitCommit(int partition, long expected) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            if (committed(partition) == expected) return;
            Thread.sleep(20);
        }
        assertEquals(expected, committed(partition));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}

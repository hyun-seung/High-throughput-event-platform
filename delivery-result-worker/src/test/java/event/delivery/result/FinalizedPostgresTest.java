package event.delivery.result;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import event.common.lifecycle.DeliveryFinalized;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "POSTGRES_TEST_URL", matches = ".+")
class FinalizedPostgresTest {
    final String schema = "result_test_" + UUID.randomUUID().toString().replace("-", "");
    final FinalizedCodec codec = new FinalizedCodec();
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    HikariDataSource pool;
    JdbcTemplate jdbc;
    FinalizedStore store;
    String url, user, password;

    @BeforeEach void connect() {
        url = System.getenv("POSTGRES_TEST_URL");
        URI endpoint = URI.create(url.substring("jdbc:".length()));
        if (!Set.of("localhost", "127.0.0.1").contains(endpoint.getHost())) throw new IllegalArgumentException("Local test database required");
        user = System.getenv().getOrDefault("POSTGRES_TEST_USER", "delivery");
        password = System.getenv().getOrDefault("POSTGRES_TEST_PASSWORD", "delivery");
        migrate();
        var config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setSchema(schema);
        config.setMaximumPoolSize(8);
        config.setConnectionTimeout(3000);
        pool = new HikariDataSource(config);
        jdbc = new JdbcTemplate(pool);
        store = new FinalizedStore(jdbc, new DataSourceTransactionManager(pool), codec, meters);
    }

    private void migrate() {
        Flyway.configure().dataSource(url, user, password).defaultSchema(schema).schemas(schema).load().migrate();
    }

    @AfterEach void cleanup() {
        if (pool != null) {
            try { jdbc.execute("DROP SCHEMA " + schema + " CASCADE"); }
            finally { pool.close(); }
        }
        meters.close();
    }

    @Test void storesBothRoutesAndAllOutcomesAndMigrationIsRepeatable() {
        for (int route : new int[]{1, 2}) for (String outcome : new String[]{"DELIVERED", "FAILED", "EXPIRED"}) {
            var e = FinalizedCodecTest.event(outcome, route);
            assertEquals(FinalizedStore.Outcome.STORED, store.save(e));
            assertEquals(e, codec.stored(jdbc.queryForObject("SELECT result_json::text FROM delivery_history WHERE delivery_id = ?", String.class, UUID.fromString(e.deliveryId()))));
        }
        migrate();
        assertEquals(6, count("delivery_history"));
        assertEquals(6, count("customer_notification_outbox"));
        assertEquals(6, jdbc.queryForObject("SELECT count(*) FROM customer_notification_outbox WHERE status = 'PENDING' AND attempt_count = 0", Integer.class));
        assertEquals("on", jdbc.queryForObject("SHOW fsync", String.class));
        assertEquals("on", jdbc.queryForObject("SHOW synchronous_commit", String.class));
    }

    @Test void concurrentDuplicatesProduceOneHistoryAndOneReservation() throws Exception {
        var e = FinalizedCodecTest.event("DELIVERED", 1);
        try (var threads = Executors.newFixedThreadPool(8)) {
            var results = new ArrayList<Future<FinalizedStore.Outcome>>();
            for (int i = 0; i < 24; i++) results.add(threads.submit(() -> store.save(e)));
            int inserted = 0;
            for (var result : results) if (result.get(20, TimeUnit.SECONDS) == FinalizedStore.Outcome.STORED) inserted++;
            assertEquals(1, inserted);
        }
        assertEquals(1, count("delivery_history"));
        assertEquals(1, count("customer_notification_outbox"));
        assertEquals(23, meters.counter("delivery.result.records", "outcome", "duplicate").count());
    }

    @Test void replayDoesNotResetDeliveredOrExhaustedNotification() {
        for (String state : new String[]{"DELIVERED", "EXHAUSTED"}) {
            var e = FinalizedCodecTest.event("FAILED", 2);
            store.save(e);
            jdbc.update("UPDATE customer_notification_outbox SET status = ?, attempt_count = 21, next_attempt_at = '2030-01-01Z' WHERE result_event_id = ?", state, UUID.fromString(e.eventId()));
            var before = jdbc.queryForMap("SELECT * FROM customer_notification_outbox WHERE result_event_id = ?", UUID.fromString(e.eventId()));
            assertEquals(FinalizedStore.Outcome.DUPLICATE, store.save(e));
            assertEquals(before, jdbc.queryForMap("SELECT * FROM customer_notification_outbox WHERE result_event_id = ?", UUID.fromString(e.eventId())));
        }
    }

    @Test void conflictNeverOverwritesHistoryOrReservationIncludingSubMicrosecondDates() {
        var e = FinalizedCodecTest.event("DELIVERED", 1);
        store.save(e);
        for (DeliveryFinalized changed : List.of(
                new DeliveryFinalized(1, e.eventType(), e.eventId(), e.deliveryId(), e.tenantId() + 1, e.deliveryType(), e.outcome(), e.reason(), e.routeOrder(), e.attemptId(), e.provider(), e.occurredAt(), e.resultAt(), e.finalizedAt(), e.deadline()),
                new DeliveryFinalized(1, e.eventType(), e.eventId(), e.deliveryId(), e.tenantId(), e.deliveryType(), e.outcome(), "CHANGED", e.routeOrder(), e.attemptId(), e.provider(), e.occurredAt(), e.resultAt(), e.finalizedAt(), e.deadline()),
                new DeliveryFinalized(1, e.eventType(), e.eventId(), e.deliveryId(), e.tenantId(), e.deliveryType(), e.outcome(), e.reason(), e.routeOrder(), e.attemptId(), e.provider(), e.occurredAt(), e.resultAt().plusNanos(1), e.finalizedAt(), e.deadline()))) {
            assertThrows(FinalizedStore.Conflict.class, () -> store.save(changed));
        }
        assertEquals(e, codec.stored(jdbc.queryForObject("SELECT result_json::text FROM delivery_history", String.class)));
        assertEquals(1, count("customer_notification_outbox"));
    }

    @Test void failedReservationInsertRollsBackHistoryAndCanRecover() {
        jdbc.execute("ALTER TABLE customer_notification_outbox ADD CONSTRAINT injected_failure CHECK (attempt_count < 0)");
        var e = FinalizedCodecTest.event("EXPIRED", 2);
        assertThrows(org.springframework.dao.DataAccessException.class, () -> store.save(e));
        assertEquals(0, count("delivery_history"));
        assertEquals(0, count("customer_notification_outbox"));
        jdbc.execute("ALTER TABLE customer_notification_outbox DROP CONSTRAINT injected_failure");
        assertEquals(FinalizedStore.Outcome.STORED, store.save(e));
    }

    @Test void missingReservationIsNotRecreatedAsAnUnintendedSecondNotification() {
        var e = FinalizedCodecTest.event("DELIVERED", 1);
        store.save(e);
        jdbc.update("DELETE FROM customer_notification_outbox WHERE result_event_id = ?", UUID.fromString(e.eventId()));
        assertThrows(IllegalStateException.class, () -> store.save(e));
        assertEquals(1, count("delivery_history"));
        assertEquals(0, count("customer_notification_outbox"));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "KAFKA_TEST_BOOTSTRAP_SERVERS", matches = ".+")
    void kafkaKeepsOffsetOnSqlRollbackThenRecoversAndDeduplicatesAfterCommittedStoreRestart() throws Exception {
        try (var kafka = new KafkaFixture()) {
            var consumer = new FinalizedConsumer(codec, store, meters);
            kafka.start(consumer::consume);
            kafka.send(FinalizedCodecTest.event("DELIVERED", 1));
            kafka.awaitOffset(1);
            jdbc.execute("ALTER TABLE customer_notification_outbox ADD CONSTRAINT injected_failure CHECK (attempt_count < 0) NOT VALID");
            var second = FinalizedCodecTest.event("FAILED", 2);
            kafka.send(second);
            await(() -> meters.counter("delivery.result.records", "outcome", "failed").count() > 0);
            assertEquals(1, kafka.offset());
            assertEquals(1, count("delivery_history"));
            jdbc.execute("ALTER TABLE customer_notification_outbox DROP CONSTRAINT injected_failure");
            kafka.awaitOffset(2);
            kafka.stop();

            var committed = new CountDownLatch(1);
            kafka.start(record -> {
                consumer.consume(record);
                committed.countDown();
                throw new IllegalStateException("Injected interruption after SQL commit and before Kafka ack");
            });
            var third = FinalizedCodecTest.event("EXPIRED", 2);
            kafka.send(third);
            assertTrue(committed.await(20, TimeUnit.SECONDS));
            assertEquals(2, kafka.offset());
            assertEquals(3, count("delivery_history"));
            kafka.stop();
            kafka.start(new FinalizedConsumer(codec, new FinalizedStore(jdbc, new DataSourceTransactionManager(pool), codec, meters), meters)::consume);
            kafka.awaitOffset(3);
            assertEquals(3, count("delivery_history"));
            assertEquals(3, count("customer_notification_outbox"));
            assertTrue(meters.counter("delivery.result.records", "outcome", "duplicate").count() >= 1);
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "KAFKA_TEST_BOOTSTRAP_SERVERS", matches = ".+")
    void malformedRecordAndContentConflictKeepOffsetWithoutDropping() throws Exception {
        try (var kafka = new KafkaFixture()) {
            var consumer = new FinalizedConsumer(codec, store, meters);
            kafka.start(consumer::consume);
            var e = FinalizedCodecTest.event("DELIVERED", 1);
            kafka.send(e);
            kafka.awaitOffset(1);
            kafka.producer.send(new ProducerRecord<>(kafka.topic, e.deliveryId(), "{}".getBytes(StandardCharsets.UTF_8))).get(10, TimeUnit.SECONDS);
            await(() -> meters.counter("delivery.result.records", "outcome", "invalid").count() >= 3);
            assertEquals(1, kafka.offset());
            assertEquals(1, count("delivery_history"));
        }
        try (var kafka = new KafkaFixture()) {
            var e = FinalizedCodecTest.event("FAILED", 1);
            store.save(e);
            kafka.start(new FinalizedConsumer(codec, store, meters)::consume);
            var changed = new DeliveryFinalized(1, e.eventType(), e.eventId(), e.deliveryId(), e.tenantId(), e.deliveryType(), e.outcome(), "CHANGED", e.routeOrder(), e.attemptId(), e.provider(), e.occurredAt(), e.resultAt(), e.finalizedAt(), e.deadline());
            kafka.send(changed);
            await(() -> meters.counter("delivery.result.records", "outcome", "conflict").count() >= 3);
            assertTrue(kafka.offset() <= 0);
            assertEquals(2, count("delivery_history"));
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "KAFKA_TEST_BOOTSTRAP_SERVERS", matches = ".+")
    void applicationStartsMigratesConsumesAndExposesPrometheus() throws Exception {
        // Exercise application-owned migration from an absent schema, not only a pre-migrated database.
        jdbc.execute("DROP SCHEMA " + schema + " CASCADE");
        try (var kafka = new KafkaFixture(); var context = SpringApplication.run(DeliveryResultApplication.class,
                "--server.port=0", "--management.server.port=0", "--spring.datasource.url=" + url,
                "--spring.datasource.username=" + user, "--spring.datasource.password=" + password,
                "--spring.datasource.hikari.schema=" + schema, "--spring.flyway.default-schema=" + schema,
                "--spring.flyway.schemas=" + schema, "--result.topic=" + kafka.topic,
                "--result.concurrency=1", "--spring.kafka.bootstrap-servers=" + kafka.bootstrap,
                "--spring.kafka.consumer.group-id=" + kafka.group)) {
            kafka.send(FinalizedCodecTest.event("DELIVERED", 2));
            kafka.awaitOffset(1);
            assertEquals(1, count("delivery_history"));
            assertEquals(1, count("customer_notification_outbox"));
            int port = context.getEnvironment().getRequiredProperty("local.management.port", Integer.class);
            try (var http = HttpClient.newHttpClient()) {
                var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/actuator/prometheus")).GET().build(), HttpResponse.BodyHandlers.ofString());
                assertEquals(200, response.statusCode());
                assertTrue(response.body().contains("delivery_result_records_total"));
                assertTrue(response.body().contains("outcome=\"stored\""));
            }
        }
    }

    private int count(String table) { return Objects.requireNonNull(jdbc.queryForObject("SELECT count(*) FROM " + table, Integer.class)); }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long end = System.nanoTime() + Duration.ofSeconds(25).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < end) Thread.sleep(25);
        assertTrue(condition.getAsBoolean(), "Condition did not become true before timeout");
    }

    private class KafkaFixture implements AutoCloseable {
        final String topic = "result-test-" + UUID.randomUUID(), group = topic + "-consumer";
        final String bootstrap = System.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS");
        final Admin admin;
        final KafkaProducer<String, byte[]> producer;
        ConcurrentMessageListenerContainer<String, byte[]> container;

        KafkaFixture() throws Exception {
            for (String endpoint : bootstrap.split(",")) {
                if (!Set.of("localhost", "127.0.0.1").contains(endpoint.split(":")[0])) throw new IllegalArgumentException("Local Kafka required");
            }
            admin = Admin.create(Map.of("bootstrap.servers", bootstrap));
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            producer = new KafkaProducer<>(Map.of("bootstrap.servers", bootstrap, "acks", "all"), new StringSerializer(), new ByteArraySerializer());
        }
        void start(MessageListener<String, byte[]> listener) {
            var props = new KafkaProperties();
            props.setBootstrapServers(List.of(bootstrap));
            props.getConsumer().setGroupId(group);
            props.getConsumer().setAutoOffsetReset("earliest");
            var factory = new ResultConsumerConfiguration().finalizedListenerContainerFactory(props, 1, 100);
            container = factory.createContainer(topic);
            container.getContainerProperties().setPollTimeout(100);
            container.setupMessageListener(listener);
            container.start();
        }
        void stop() { if (container != null) { container.stop(); container = null; } }
        void send(DeliveryFinalized e) throws Exception {
            producer.send(new ProducerRecord<>(topic, e.deliveryId(), codec.encode(e).getBytes(StandardCharsets.UTF_8))).get(10, TimeUnit.SECONDS);
        }
        long offset() {
            try {
                var offset = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(new TopicPartition(topic, 0));
                return offset == null ? -1 : offset.offset();
            } catch (Exception e) { throw new IllegalStateException(e); }
        }
        void awaitOffset(long expected) throws InterruptedException { await(() -> offset() == expected); }
        @Override public void close() throws Exception {
            stop();
            producer.close();
            try {
                admin.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
                admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
            } finally { admin.close(); }
        }
    }
}

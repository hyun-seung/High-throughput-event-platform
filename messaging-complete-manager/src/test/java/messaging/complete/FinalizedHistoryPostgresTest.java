package messaging.complete;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import messaging.common.messages.FinalizedMessageResult;
import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.PrimaryStageDecision;
import messaging.common.messages.SecondaryStageDecision;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.Instant;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@EnabledIfEnvironmentVariable(named = "POSTGRES_TEST_URL", matches = ".+")
class FinalizedHistoryPostgresTest {
    private final String schema = "completion_test_" + UUID.randomUUID().toString().replace("-", "");
    private final JsonMapper mapper = JsonMapper.builder().build();
    private HikariDataSource pool;
    private JdbcTemplate jdbc;
    private FinalizedHistoryStore history;
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();

    @BeforeEach
    void connect() {
        String url = System.getenv("POSTGRES_TEST_URL");
        URI endpoint = URI.create(url.substring("jdbc:".length()));
        if (!Set.of("localhost", "127.0.0.1").contains(endpoint.getHost())) {
            throw new IllegalArgumentException("Local test database required");
        }
        String user = System.getenv().getOrDefault("POSTGRES_TEST_USER", "delivery");
        String password = System.getenv().getOrDefault("POSTGRES_TEST_PASSWORD", "delivery");
        Flyway.configure().dataSource(url, user, password).defaultSchema(schema).schemas(schema).load().migrate();
        var config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setSchema(schema);
        config.addDataSourceProperty("reWriteBatchedInserts", "true");
        pool = new HikariDataSource(config);
        jdbc = new JdbcTemplate(pool);
        history = new FinalizedHistoryStore(jdbc, new DataSourceTransactionManager(pool), mapper, metrics);
    }

    @AfterEach
    void cleanup() {
        if (pool == null) return;
        try { jdbc.execute("DROP SCHEMA " + schema + " CASCADE"); }
        finally { pool.close(); }
    }

    @Test
    void successCreatesOneMessageAndOneCdrWhileFailureHasNoCdr() {
        var success = result("a".repeat(32), PrimaryStageDecision.Kind.SUCCESS);
        var failure = result("b".repeat(32), PrimaryStageDecision.Kind.FAILURE);

        history.store(success);
        history.store(success);
        history.store(failure);
        history.store(failure);

        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
        assertEquals(2, jdbc.queryForObject(
                "SELECT count(*) FROM tbl_msg_hist WHERE cleanup_status = 'PENDING'", Integer.class));
        assertEquals(success.decision().decisionId(), jdbc.queryForObject(
                "SELECT decision_id FROM tbl_cdr_hist WHERE client_msg_id = ?", String.class,
                success.decision().clientMsgId()));
    }

    @Test
    void primaryHistoryRetainsFinalCarrierAndInvocationAfterRoutingOrRetry() {
        for (var kind : PrimaryStageDecision.Kind.values()) {
            var original = result(UUID.randomUUID().toString().replace("-", ""), kind);
            var decision = original.decision();
            var finalized = new FinalizedMessageResult(new PrimaryStageDecision(
                    decision.decisionId(), decision.clientMsgId(), kind, decision.source(),
                    decision.errorCode(), decision.reason(), HttpCarrier.KT, 4, false,
                    decision.decidedAt()), original.submission());

            history.store(finalized);
            history.store(finalized);

            assertEquals("KT", jdbc.queryForObject(
                    "SELECT carrier FROM tbl_msg_hist WHERE client_msg_id = ?", String.class,
                    decision.clientMsgId()));
            assertEquals(4, jdbc.queryForObject(
                    "SELECT invocation FROM tbl_msg_hist WHERE client_msg_id = ?", Integer.class,
                    decision.clientMsgId()));
        }
    }

    @Test
    void failureBeforeCarrierSelectionStoresNoCarrierOrInvocation() {
        var original = result("f".repeat(32), PrimaryStageDecision.Kind.FAILURE);
        var decision = original.decision();
        var finalized = new FinalizedMessageResult(new PrimaryStageDecision(
                decision.decisionId(), decision.clientMsgId(), decision.kind(), "PRE_SEND",
                40004, "contract unavailable", null, null, false, decision.decidedAt()), original.submission());

        history.store(finalized);

        assertNull(jdbc.queryForObject("SELECT carrier FROM tbl_msg_hist", String.class));
        assertNull(jdbc.queryForObject("SELECT invocation FROM tbl_msg_hist", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
    }

    @Test
    void secondarySuccessStoresOneFinalHistoryWithoutBilling() {
        String id = "d".repeat(32);
        var now = Instant.parse("2026-10-07T12:00:00Z");
        var primary = new PrimaryStageDecision("primary-failure", id,
                PrimaryStageDecision.Kind.FAILURE, "HTTP_RESPONSE", 66999, null,
                HttpCarrier.SKT, 1, true, now);
        var secondary = new SecondaryStageDecision("tcp-result", id, "tcp-attempt",
                SecondaryStageDecision.Kind.SUCCESS, null, "RECEIVED", now.plusSeconds(1));
        var submission = new MessageSubmission(id, 42L, "customer-message", "01012345678",
                MessageCategory.GENERAL, Map.of("text", "hello"), Map.of("text", "secondary"), now.minusSeconds(30));
        var finalized = new FinalizedMessageResult(primary, secondary, submission);

        history.store(finalized);
        history.store(finalized);

        assertEquals("SECONDARY", jdbc.queryForObject("SELECT final_stage FROM tbl_msg_hist WHERE client_msg_id = ?", String.class, id));
        assertEquals("SUCCESS", jdbc.queryForObject("SELECT outcome FROM tbl_msg_hist WHERE client_msg_id = ?", String.class, id));
        assertEquals("SKT", jdbc.queryForObject("SELECT carrier FROM tbl_msg_hist WHERE client_msg_id = ?", String.class, id));
        assertNull(jdbc.queryForObject("SELECT invocation FROM tbl_msg_hist WHERE client_msg_id = ?", Integer.class, id));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
    }

    @Test
    void dynamoFailureKeepsSqlCleanupPendingForRetry() {
        var finalized = result("e".repeat(32), PrimaryStageDecision.Kind.SUCCESS);
        history.store(finalized);
        var db = mock(DynamoDbClient.class);
        when(db.getItem(any(GetItemRequest.class))).thenThrow(new IllegalStateException("offline"));

        new FinalizedDynamoCleanup(jdbc, db, Clock.systemUTC(), 10, 4, Duration.ofDays(7), new io.micrometer.core.instrument.simple.SimpleMeterRegistry()).poll();

        assertEquals("PENDING", jdbc.queryForObject(
                "SELECT cleanup_status FROM tbl_msg_hist WHERE client_msg_id = ?", String.class,
                finalized.submission().clientMsgId()));
        assertEquals(1, jdbc.queryForObject(
                "SELECT cleanup_attempts FROM tbl_msg_hist WHERE client_msg_id = ?", Integer.class,
                finalized.submission().clientMsgId()));
    }

    @Test
    void conflictingFinalResultCannotChangeCommittedHistoryOrBilling() {
        var original = result("a".repeat(32), PrimaryStageDecision.Kind.SUCCESS);
        history.store(original);
        var conflicting = new FinalizedMessageResult(new PrimaryStageDecision(
                "other-decision", original.decision().clientMsgId(), PrimaryStageDecision.Kind.FAILURE,
                "WEBHOOK", 66001, null, HttpCarrier.SKT, 1, false,
                original.decision().decidedAt()), original.submission());

        assertThrows(IllegalStateException.class, () -> history.store(conflicting));

        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
        assertEquals(original.decision().decisionId(), jdbc.queryForObject(
                "SELECT decision_id FROM tbl_msg_hist WHERE client_msg_id = ?", String.class,
                original.decision().clientMsgId()));
    }

    @Test
    void concurrentRedeliveryStillCreatesOneMessageAndOneCdr() throws Exception {
        var finalized = result("c".repeat(32), PrimaryStageDecision.Kind.SUCCESS);
        try (var threads = Executors.newFixedThreadPool(8)) {
            var deliveries = new ArrayList<Future<?>>();
            for (int i = 0; i < 24; i++) deliveries.add(threads.submit(() -> history.store(finalized)));
            for (var delivery : deliveries) delivery.get(20, TimeUnit.SECONDS);
        }

        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
    }

    @Test
    void mixedBatchBillsOnlyPrimarySuccessAndReplayDoesNotResetCleanup() {
        var batch = new ArrayList<FinalizedMessageResult>();
        for (int i = 0; i < 100; i++) {
            String id = "mixed-" + i;
            batch.add(i < 80 ? result(id, PrimaryStageDecision.Kind.SUCCESS)
                    : i < 90 ? result(id, PrimaryStageDecision.Kind.FAILURE) : secondary(id));
        }
        history.storeBatch(batch);
        jdbc.update("UPDATE tbl_msg_hist SET cleanup_status='DONE', cleaned_at=now() WHERE client_msg_id='mixed-0'");
        Collections.reverse(batch);
        history.storeBatch(batch);

        assertEquals(100, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(80, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
        assertEquals(10, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist WHERE final_stage='SECONDARY'", Integer.class));
        assertEquals("DONE", jdbc.queryForObject("SELECT cleanup_status FROM tbl_msg_hist WHERE client_msg_id='mixed-0'", String.class));
        assertEquals(2, metrics.get("messaging.complete.sql.batch.size").summary().count());
        assertEquals(200, metrics.get("messaging.complete.sql.batch.size").summary().totalAmount());
    }

    @Test
    void identicalDuplicatesWithinOneBatchCreateOneHistoryAndCdr() {
        var one = result("same", PrimaryStageDecision.Kind.SUCCESS);
        history.storeBatch(List.of(one, one, one));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
    }

    @Test
    void conflictingRecordRollsBackOtherNewMessagesInTheBatch() {
        history.store(result("existing", PrimaryStageDecision.Kind.SUCCESS));
        assertThrows(IllegalStateException.class, () -> history.storeBatch(List.of(
                result("new", PrimaryStageDecision.Kind.SUCCESS),
                result("existing", PrimaryStageDecision.Kind.FAILURE))));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
        assertEquals("SUCCESS", jdbc.queryForObject("SELECT outcome FROM tbl_msg_hist", String.class));
    }

    @Test
    void conflictingDuplicatesInsideTheSameBatchRollbackEverything() {
        assertThrows(IllegalStateException.class, () -> history.storeBatch(List.of(
                result("same", PrimaryStageDecision.Kind.SUCCESS), result("same", PrimaryStageDecision.Kind.FAILURE))));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
    }

    @Test
    void cdrInsertFailureRollsBackHistoryAndOtherBillableRecordsThenCanBeRetried() {
        jdbc.execute("ALTER TABLE tbl_cdr_hist ADD CONSTRAINT reject_test CHECK (client_msg_id <> 'reject')");
        var batch = List.of(result("allowed", PrimaryStageDecision.Kind.SUCCESS),
                result("reject", PrimaryStageDecision.Kind.SUCCESS));
        assertThrows(org.springframework.dao.DataIntegrityViolationException.class, () -> history.storeBatch(batch));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
        jdbc.execute("ALTER TABLE tbl_cdr_hist DROP CONSTRAINT reject_test");
        history.storeBatch(batch);
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
    }

    @Test
    void conflictingCdrPreventsCommittingNewRecords() {
        var existing = result("existing", PrimaryStageDecision.Kind.SUCCESS);
        history.store(existing);
        jdbc.update("UPDATE tbl_cdr_hist SET decision_id='unexpected' WHERE client_msg_id='existing'");
        assertThrows(IllegalStateException.class, () -> history.storeBatch(List.of(existing,
                result("new", PrimaryStageDecision.Kind.SUCCESS))));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
    }

    @Test
    void concurrentOverlappingBatchesUseStableLockOrderAndRemainIdempotent() throws Exception {
        var batch = new ArrayList<FinalizedMessageResult>();
        for (int i = 0; i < 50; i++) batch.add(result("overlap-" + i, PrimaryStageDecision.Kind.SUCCESS));
        var reverse = new ArrayList<>(batch);
        Collections.reverse(reverse);
        try (var threads = Executors.newFixedThreadPool(4)) {
            var futures = new ArrayList<Future<?>>();
            for (int i = 0; i < 8; i++) {
                var selected = i % 2 == 0 ? batch : reverse;
                futures.add(threads.submit(() -> history.storeBatch(selected)));
            }
            for (var future : futures) future.get(20, TimeUnit.SECONDS);
        }
        assertEquals(50, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(50, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
    }

    @Test
    void emptyBatchDoesNothingAndOversizedBatchDoesNotPartiallyCommit() {
        history.storeBatch(List.of());
        assertThrows(IllegalArgumentException.class, () -> history.storeBatch(Collections.nCopies(101,
                result("too-many", PrimaryStageDecision.Kind.SUCCESS))));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(0, metrics.get("messaging.complete.sql.batch.duration").timer().count());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "KAFKA_TEST_BOOTSTRAP_SERVERS", matches = ".+")
    void kafkaDoesNotCommitBeforeSqlAndReplaysTheCommitOffsetGapWithoutDoubleBilling() throws Exception {
        String brokers = System.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS");
        if (!brokers.matches("(localhost|127\\.0\\.0\\.1):[0-9]+")) throw new IllegalArgumentException("Local test Kafka required");
        String topic = "completion-test-" + UUID.randomUUID();
        String group = topic + "-group";
        var partition = new TopicPartition(topic, 0);
        var records = new ArrayList<FinalizedMessageResult>();
        for (int i = 0; i < 100; i++) records.add(result("kafka-" + i, PrimaryStageDecision.Kind.SUCCESS));
        jdbc.execute("ALTER TABLE tbl_cdr_hist ADD CONSTRAINT reject_kafka CHECK (client_msg_id <> 'kafka-99')");
        var failed = new CountDownLatch(1);
        var sqlCommitted = new CountDownLatch(1);
        var allowOffset = new AtomicBoolean(false);
        var listener = new FinalizedResultConsumer(history, mapper);
        var props = new ContainerProperties(topic);
        props.setGroupId(group);
        props.setAckMode(ContainerProperties.AckMode.BATCH);
        props.setMessageListener((BatchMessageListener<String, String>) batch -> {
            try { listener.receive(batch); }
            catch (RuntimeException failure) { failed.countDown(); throw failure; }
            sqlCommitted.countDown();
            if (!allowOffset.get()) throw new IllegalStateException("Simulated gap between SQL commit and Kafka offset");
        });
        var factory = new DefaultKafkaConsumerFactory<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 16384,
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 25,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100));
        var container = new KafkaMessageListenerContainer<>(factory, props);
        container.setCommonErrorHandler(new CompleteManagerConfiguration().completeErrorHandler());
        try (var admin = Admin.create(Map.of("bootstrap.servers", brokers));
             var producer = new KafkaProducer<String, String>(Map.of("bootstrap.servers", brokers,
                     "key.serializer", StringSerializer.class, "value.serializer", StringSerializer.class))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            try {
                for (var record : records) producer.send(new ProducerRecord<>(topic, record.submission().clientMsgId(),
                        mapper.writeValueAsString(record))).get(10, TimeUnit.SECONDS);
                container.start();
                assertTrue(failed.await(30, TimeUnit.SECONDS));
                assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
                assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
                assertEquals(0, committed(admin, group, partition));

                jdbc.execute("ALTER TABLE tbl_cdr_hist DROP CONSTRAINT reject_kafka");
                assertTrue(sqlCommitted.await(30, TimeUnit.SECONDS));
                assertEquals(100, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
                assertEquals(100, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
                assertEquals(0, committed(admin, group, partition));

                // Restart with no committed offset: the persisted batch must be replayed safely.
                container.stop();
                allowOffset.set(true);
                container.start();
                awaitCondition(() -> committed(admin, group, partition) == 100);
                assertEquals(100, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
                assertEquals(100, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
                assertTrue(metrics.get("messaging.complete.sql.batch.size").summary().count() >= 2);
                assertEquals(100, metrics.get("messaging.complete.sql.batch.size").summary().max());
                var one = result("single-after-batch", PrimaryStageDecision.Kind.SUCCESS);
                producer.send(new ProducerRecord<>(topic, one.submission().clientMsgId(), mapper.writeValueAsString(one)))
                        .get(10, TimeUnit.SECONDS);
                awaitCondition(() -> committed(admin, group, partition) == 101);
                assertEquals(101, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
                assertEquals(101, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
            } finally {
                container.stop();
                admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
                admin.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
            }
        }
    }

    private static long committed(Admin admin, String group, TopicPartition partition) {
        try {
            var offset = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS).get(partition);
            return offset == null ? 0 : offset.offset();
        } catch (Exception failure) { throw new IllegalStateException(failure); }
    }

    private static void awaitCondition(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(100);
        }
        fail("Condition not met within 30 seconds");
    }

    @Test
    void measuresSingleAndBatchTransactionsWithoutAssumingATimingThreshold() {
        // Warm both SQL shapes; report samples, never turn host-dependent timings into a flaky assertion.
        var warm = new ArrayList<FinalizedMessageResult>();
        for (int i = 0; i < 100; i++) warm.add(result("warm-" + i, PrimaryStageDecision.Kind.SUCCESS));
        warm.forEach(history::store);
        history.storeBatch(warm);
        for (int round = 0; round < 3; round++) {
            var single = new ArrayList<FinalizedMessageResult>();
            var batch = new ArrayList<FinalizedMessageResult>();
            for (int i = 0; i < 100; i++) {
                single.add(result("single-" + round + "-" + i, PrimaryStageDecision.Kind.SUCCESS));
                batch.add(result("batch-" + round + "-" + i, PrimaryStageDecision.Kind.SUCCESS));
            }
            long singleNanos, batchNanos;
            if (round % 2 == 0) {
                long start = System.nanoTime(); single.forEach(history::store); singleNanos = System.nanoTime() - start;
                start = System.nanoTime(); history.storeBatch(batch); batchNanos = System.nanoTime() - start;
            } else {
                long start = System.nanoTime(); history.storeBatch(batch); batchNanos = System.nanoTime() - start;
                start = System.nanoTime(); single.forEach(history::store); singleNanos = System.nanoTime() - start;
            }
            System.out.printf("COMPLETION_SQL_SAMPLE round=%d rows=100 singleMs=%.3f batchMs=%.3f%n",
                    round, singleNanos / 1e6, batchNanos / 1e6);
        }
        assertEquals(700, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(700, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
        assertEquals(404, metrics.get("messaging.complete.sql.batch.size").summary().count());
    }

    private static FinalizedMessageResult secondary(String id) {
        var original = result(id, PrimaryStageDecision.Kind.FAILURE);
        var d = original.decision();
        return new FinalizedMessageResult(new PrimaryStageDecision(d.decisionId(), id, d.kind(), d.source(),
                d.errorCode(), d.reason(), d.carrier(), d.invocation(), true, d.decidedAt()),
                new SecondaryStageDecision("tcp-" + id, id, "tcp-attempt", SecondaryStageDecision.Kind.SUCCESS,
                        null, "RECEIVED", d.decidedAt().plusSeconds(1)), original.submission());
    }

    private static FinalizedMessageResult result(String id, PrimaryStageDecision.Kind kind) {
        var now = Instant.parse("2026-10-07T12:00:00Z");
        var decision = new PrimaryStageDecision("decision-" + id, id, kind, "WEBHOOK",
                kind == PrimaryStageDecision.Kind.FAILURE ? 66999 : null, null,
                HttpCarrier.SKT, 1, false, now);
        var submission = new MessageSubmission(id, 42L, "customer-message", "01012345678",
                MessageCategory.GENERAL, Map.of("text", "hello"), null, now.minusSeconds(30));
        return new FinalizedMessageResult(decision, submission);
    }
}

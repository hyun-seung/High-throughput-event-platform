package event.delivery.ingress.dlt;

import event.common.delivery.*;
import event.common.dynamodb.config.DynamoDbTableInitializer;
import event.common.lifecycle.DeliveryCompletion;
import event.delivery.ingress.repository.DeliveryRepository;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static event.delivery.ingress.dlt.DltRecoveryPlanner.Decision.*;
import static org.junit.jupiter.api.Assertions.*;

/** Own SQL schema, UUID DDB keys and Kafka topic. Assertions use actual persisted state. */
@EnabledIfEnvironmentVariable(named = "DYNAMODB_TEST_ENDPOINT", matches = ".+")
@EnabledIfEnvironmentVariable(named = "POSTGRES_TEST_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "KAFKA_TEST_BOOTSTRAP_SERVERS", matches = ".+")
class DltRecoveryIntegrationTest {
    static final Instant NOW = DltInspectionTest.NOW;
    static final JsonMapper MAPPER = JsonMapper.builder().build();
    static DynamoDbClient db;
    static String schema, bootstrap;
    DltRecoveryStore store;
    DeliveryEvent admitted, execution;
    ConsumerRecord<byte[], byte[]> record;
    String topic;
    String inputTopic;
    Admin admin;
    KafkaProducer<String, byte[]> producer;

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(System.getenv("POSTGRES_TEST_URL"),
                System.getenv().getOrDefault("POSTGRES_TEST_USER", "delivery"),
                System.getenv().getOrDefault("POSTGRES_TEST_PASSWORD", "delivery"));
    }

    @BeforeAll static void storage() throws Exception {
        URI endpoint = URI.create(System.getenv("DYNAMODB_TEST_ENDPOINT"));
        URI sql = URI.create(System.getenv("POSTGRES_TEST_URL").replaceFirst("^jdbc:", ""));
        bootstrap = System.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS");
        for (URI uri : List.of(endpoint, sql)) {
            if (!Set.of("localhost", "127.0.0.1").contains(uri.getHost())) throw new IllegalArgumentException("Local only");
        }
        if (!bootstrap.matches("(localhost|127\\.0\\.0\\.1):[0-9]+")) throw new IllegalArgumentException("Local broker only");
        schema = "dlt_test_" + UUID.randomUUID().toString().replace("-", "");
        try (var connection = connect(); var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
            statement.execute("SET search_path TO " + schema);
            for (String migration : List.of("V1__delivery_history_and_notification_outbox.sql", "V2__customer_notification_batches.sql",
                    "V3__delivery_cleanup_reservations.sql", "V4__dlt_recovery_handoff.sql", "V5__dlt_history_coverage.sql",
                    "V6__dlt_recovery_checkpoint.sql", "V7__dlt_intake.sql", "V8__dlt_operator_recheck.sql")) {
                try (var resource = DltRecoveryIntegrationTest.class.getResourceAsStream("/db/migration/" + migration)) {
                    assertNotNull(resource); statement.execute(new String(resource.readAllBytes(), StandardCharsets.UTF_8));
                }
            }
        }
        db = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(10))).build();
        new DynamoDbTableInitializer(db).run(null);
    }

    @AfterAll static void closeStorage() throws Exception {
        if (db != null) db.close();
        if (schema != null) try (var connection = connect(); var statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA " + schema + " CASCADE");
        }
    }

    @BeforeEach void setup() throws Exception {
        try (var connection = connect(); var statement = connection.createStatement()) {
            statement.execute("UPDATE " + schema + ".dlt_history_coverage SET origin_restore_enabled=false");
        }
        store = new DltRecoveryStore(DltRecoveryIntegrationTest::connect, schema);
        admitted = DeliveryEvent.requested(UUID.randomUUID().toString(), 999L, "SMS", Map.of("text", "private-payload"), NOW, false).forAdmission();
        execution = new DeliveryRepository(db, MAPPER).saveOrLoad(admitted).event();
        var template = DltInspectionTest.record(admitted);
        record = new ConsumerRecord<>("dlt", 0, 5, admitted.requestKey().getBytes(StandardCharsets.UTF_8), template.value());
        template.headers().forEach(h -> record.headers().add(h));
        // Unique cluster aliases below distinguish tests using the same synthetic source coordinate.
        topic = "test.dlt-recovery." + UUID.randomUUID();
        admin = Admin.create(Map.of("bootstrap.servers", bootstrap));
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        producer = new KafkaProducer<>(Map.of("bootstrap.servers", bootstrap, "acks", "all"), new StringSerializer(), new ByteArraySerializer());
    }

    @AfterEach void cleanup() throws Exception {
        if (producer != null) producer.close(Duration.ofSeconds(5));
        if (admin != null) {
            try { admin.deleteTopics(inputTopic == null ? List.of(topic) : List.of(topic, inputTopic)).all().get(10, TimeUnit.SECONDS); }
            finally { admin.close(Duration.ofSeconds(5)); }
        }
        if (admitted != null) db.deleteItem(r -> r.tableName("ORIGIN").key(DeliveryCompletion.metaKey(admitted.requestKey())));
        if (execution != null) db.deleteItem(r -> r.tableName("STEP").key(stepKey()));
    }

    DltRecoveryPlanner planner(Instant now) {
        return new DltRecoveryPlanner(db, store, Duration.ofHours(3), "source", Clock.fixed(now, ZoneOffset.UTC));
    }
    Map<String, AttributeValue> stepKey() {
        return Map.of("pk", AttributeValue.fromS("DELIVERY#" + execution.deliveryId()), "sk", AttributeValue.fromS("ATTEMPT#test"));
    }
    DltRecoveryStore.Ack publish(DeliveryEvent command) throws Exception {
        var ack = producer.send(new ProducerRecord<>(topic, command.deliveryId(), MAPPER.writeValueAsBytes(command))).get(10, TimeUnit.SECONDS);
        return new DltRecoveryStore.Ack(ack.topic(), ack.partition(), ack.offset());
    }
    long records() throws Exception {
        var tp = new TopicPartition(topic, 0);
        return admin.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get(10, TimeUnit.SECONDS).get(tp).offset();
    }
    long count(String suffix) throws Exception {
        try (var connection = connect(); var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT count(*) FROM " + schema + "." + suffix)) {
            result.next(); return result.getLong(1);
        }
    }
    void history() throws Exception {
        try (var connection = connect(); var insert = connection.prepareStatement("INSERT INTO " + schema + ".delivery_history "
                + "(delivery_id,result_event_id,tenant_id,delivery_type,outcome,reason,route_order,attempt_id,provider,"
                + "occurred_at,result_at,finalized_at,deadline,result_json) VALUES (?, ?, 999, 'SMS','EXPIRED','EXPIRED',1,?,'test',"
                + "now(),now(),now(),now(),?::jsonb)")) {
            insert.setObject(1, UUID.fromString(execution.deliveryId())); insert.setObject(2, UUID.randomUUID());
            insert.setObject(3, UUID.randomUUID()); insert.setString(4, MAPPER.writeValueAsString(Map.of("requestKey", admitted.requestKey())));
            insert.executeUpdate();
        }
    }

    void enableRestore(Instant since) throws Exception {
        try (var connection = connect(); var statement = connection.prepareStatement("UPDATE " + schema
                + ".dlt_history_coverage SET complete_since=?,origin_restore_enabled=true")) {
            statement.setTimestamp(1, Timestamp.from(since)); statement.executeUpdate();
        }
    }
    void removeOrigin() {
        db.deleteItem(r -> r.tableName("ORIGIN").key(DeliveryCompletion.metaKey(admitted.requestKey())));
    }
    Map<String, AttributeValue> origin() {
        return db.getItem(r -> r.tableName("ORIGIN").key(DeliveryCompletion.metaKey(admitted.requestKey())).consistentRead(true)).item();
    }

    DltOperations operations() { return new DltOperations(DltRecoveryIntegrationTest::connect, schema, topic); }
    DltOperations.Item heldItem() throws Exception {
        createInput(); input(record); removeOrigin();
        intakeWorker(planner(NOW)).runOnce(0, 10);
        return operations().held(inputTopic, 0, -1, 10).records().getFirst();
    }

    @Test void heldMetadataPagesAreBoundedAndDoNotExposeOrModifyRawRecords() throws Exception {
        createInput(); input(record); input(record); input(record); removeOrigin();
        intakeWorker(planner(NOW)).runOnce(0, 10);
        var operations = operations(); var page = operations.held(inputTopic, 0, -1, 2);
        assertEquals(List.of(0L, 1L), page.records().stream().map(DltOperations.Item::offset).toList());
        assertTrue(page.hasMore()); assertEquals(1L, page.nextAfterOffset());
        var last = operations.held(inputTopic, 0, page.nextAfterOffset(), 2);
        assertEquals(2L, last.records().getFirst().offset()); assertFalse(last.hasMore()); assertNull(last.nextAfterOffset());
        assertEquals(page, operations.held(inputTopic, 0, -1, 2));
        assertFalse(MAPPER.writeValueAsString(page).contains("private-payload"));
        assertEquals(3, intakeStore().cursor(intakeScope(), 0)); assertEquals(0, records());
        assertEquals(0, count("dlt_intake_action a JOIN " + schema + ".dlt_intake_record i ON i.intake_id=a.intake_id WHERE i.cluster_alias='" + topic + "'"));
        assertThrows(IllegalArgumentException.class, () -> operations.held(inputTopic, 0, -1, 101));
        assertThrows(IllegalArgumentException.class, () -> operations.held(inputTopic, 0, -2, 10));
    }

    @Test void statusJoinsRecoveryAttemptsWithoutExposingCheckpointOrPayload() throws Exception {
        createInput(); input(record); intakeWorker(planner(NOW)).runOnce(0, 10);
        UUID id;
        try (var connection = connect(); var query = connection.prepareStatement("SELECT intake_id FROM " + schema + ".dlt_intake_record WHERE cluster_alias=?")) {
            query.setString(1, topic); try (var row = query.executeQuery()) { assertTrue(row.next()); id = row.getObject(1, UUID.class); }
        }
        var status = operations().status(id);
        assertEquals("REGISTERED", status.record().state()); assertEquals("ACKNOWLEDGED", status.record().recoveryState());
        assertEquals(1L, status.nextScanOffset()); assertEquals(1, status.recentAttempts().size());
        assertEquals("ACKNOWLEDGED", status.recentAttempts().getFirst().outcome());
        assertTrue(status.recentActions().isEmpty());
        var json = MAPPER.writeValueAsString(status);
        assertFalse(json.contains("private-payload")); assertFalse(json.contains("checkpoint_json")); assertFalse(json.contains("record_json"));
        assertEquals(status, operations().status(id)); assertEquals(1, records());
    }

    @Test void auditedRecheckRecoversArchivedHeldRecordAfterKafkaRetention() throws Exception {
        var held = heldItem(); enableRestore(NOW.minusSeconds(1));
        var action = UUID.randomUUID();
        assertEquals("QUEUED", operations().recheck(held.intakeId(), held.updatedAt(), action, "operator", "coverage verified").status());
        assertEquals("NEW", operations().status(held.intakeId()).record().state()); assertEquals(0, records());
        assertEquals(1, intakeStore().cursor(intakeScope(), 0));
        admin.deleteRecords(Map.of(new TopicPartition(inputTopic, 0), RecordsToDelete.beforeOffset(1))).all().get(10, TimeUnit.SECONDS);
        intakeWorker(planner(NOW.plus(Duration.ofHours(8)))).runOnce(0, 10);
        var status = operations().status(held.intakeId());
        assertEquals("REGISTERED", status.record().state()); assertEquals("ACKNOWLEDGED", status.record().recoveryState());
        assertEquals(action, status.recentActions().getFirst().actionId());
        assertEquals("operator", status.recentActions().getFirst().actor());
        assertEquals("coverage verified", status.recentActions().getFirst().reason());
        assertEquals(held.updatedAt(), status.recentActions().getFirst().expectedUpdatedAt());
        var command = MAPPER.readValue(DltInspector.fetchExact(bootstrap, topic, 0, 0).value(), DeliveryEvent.class);
        assertEquals(NOW, command.occurredAt()); assertFalse(command.fallbackAllowed());
    }

    @Test void repeatedActionDoesNotRequeueARecordThatWasHeldAgain() throws Exception {
        var held = heldItem(); var action = UUID.randomUUID(); var operations = operations();
        assertEquals("QUEUED", operations.recheck(held.intakeId(), held.updatedAt(), action, "operator", "check again").status());
        intakeWorker(planner(NOW)).runOnce(0, 10); // coverage still disabled; recheck cannot override it
        var after = operations.status(held.intakeId()); assertEquals("HELD", after.record().state());
        assertEquals("ALREADY_QUEUED", operations.recheck(held.intakeId(), held.updatedAt(), action, "operator", "check again").status());
        assertEquals(after, operations.status(held.intakeId())); assertEquals(1, after.recentActions().size()); assertEquals(0, records());
    }

    @Test void staleVersionAndChangedActionIdentityAreRejected() throws Exception {
        var held = heldItem(); var action = UUID.randomUUID(); var operations = operations();
        assertThrows(IllegalArgumentException.class, () -> operations.recheck(held.intakeId(), held.updatedAt().plusNanos(1), action, "operator", "rounded version"));
        assertEquals("QUEUED", operations.recheck(held.intakeId(), held.updatedAt(), action, "operator", "verified").status());
        assertEquals("ACTION_CONFLICT", operations.recheck(held.intakeId(), held.updatedAt(), action, "operator", "changed reason").status());
        intakeWorker(planner(NOW)).runOnce(0, 10);
        assertEquals("STALE_OR_NOT_HELD", operations.recheck(held.intakeId(), held.updatedAt(), UUID.randomUUID(), "operator", "old view").status());
        assertEquals(1, operations.status(held.intakeId()).recentActions().size());
        assertThrows(IllegalArgumentException.class, () -> operations.recheck(held.intakeId(), held.updatedAt(), UUID.randomUUID(), "operator", ""));
        assertEquals(0, records());
    }

    @Test void concurrentOperatorsQueueOnlyOneRecheckForTheObservedVersion() throws Exception {
        var held = heldItem(); var start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var tasks = new ArrayList<Future<DltOperations.Recheck>>();
            for (int i = 0; i < 2; i++) tasks.add(executor.submit(() -> {
                assertTrue(start.await(10, TimeUnit.SECONDS));
                return operations().recheck(held.intakeId(), held.updatedAt(), UUID.randomUUID(), "operator", "concurrent review");
            }));
            start.countDown();
            var statuses = new HashSet<String>();
            for (var task : tasks) statuses.add(task.get(10, TimeUnit.SECONDS).status());
            assertEquals(Set.of("QUEUED", "STALE_OR_NOT_HELD"), statuses);
            assertEquals(1, operations().status(held.intakeId()).recentActions().size()); assertEquals(0, records());
        }
    }

    @Test void operationsRemainScopedToConfiguredCluster() throws Exception {
        var held = heldItem();
        var other = new DltOperations(DltRecoveryIntegrationTest::connect, schema, "another-cluster");
        assertTrue(other.held(inputTopic, 0, -1, 10).records().isEmpty()); assertNull(other.status(held.intakeId()));
        assertEquals("STALE_OR_NOT_HELD", other.recheck(held.intakeId(), held.updatedAt(), UUID.randomUUID(), "operator", "wrong scope").status());
        assertEquals("HELD", operations().status(held.intakeId()).record().state());
        assertTrue(operations().status(held.intakeId()).recentActions().isEmpty());
    }

    @Test void failedAuditInsertRollsBackRecheckStateChange() throws Exception {
        var held = heldItem();
        try (var connection = connect(); var statement = connection.createStatement()) {
            statement.execute("CREATE FUNCTION " + schema + ".reject_action() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected audit failure'; END $$");
            statement.execute("CREATE TRIGGER reject_action BEFORE INSERT ON " + schema + ".dlt_intake_action FOR EACH ROW EXECUTE FUNCTION " + schema + ".reject_action()");
        }
        try {
            assertThrows(IllegalStateException.class, () -> operations().recheck(held.intakeId(), held.updatedAt(), UUID.randomUUID(), "operator", "audit unavailable"));
            var status = operations().status(held.intakeId());
            assertEquals(held, status.record()); assertTrue(status.recentActions().isEmpty()); assertEquals(0, records());
        } finally {
            try (var connection = connect(); var statement = connection.createStatement()) {
                statement.execute("DROP TRIGGER reject_action ON " + schema + ".dlt_intake_action");
                statement.execute("DROP FUNCTION " + schema + ".reject_action()");
            }
        }
    }

    DltIntakeStore intakeStore() { return new DltIntakeStore(DltRecoveryIntegrationTest::connect, schema); }
    DltIntakeStore.Scope intakeScope() { return new DltIntakeStore.Scope(topic, inputTopic, 0, "source", topic); }
    void createInput() throws Exception {
        inputTopic = topic + ".input";
        admin.createTopics(List.of(new NewTopic(inputTopic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
    }
    void input(ConsumerRecord<byte[], byte[]> record) throws Exception {
        try (var sender = new KafkaProducer<byte[], byte[]>(Map.of("bootstrap.servers", bootstrap, "acks", "all"),
                new ByteArraySerializer(), new ByteArraySerializer())) {
            sender.send(new ProducerRecord<>(inputTopic, 0, record.key(), record.value(), record.headers())).get(10, TimeUnit.SECONDS);
        }
    }
    DltIntakeWorker intakeWorker(DltRecoveryPlanner planner) {
        return new DltIntakeWorker(intakeStore(), store, planner, intakeScope(),
                (offset, limit) -> DltInspector.readRaw(bootstrap, inputTopic, 0, offset, limit), this::publish);
    }

    @Test void intakeArchivesMixedRecordsAndDeduplicatesSourceWithoutChangingConsumerGroup() throws Exception {
        createInput(); input(record); input(record);
        var malformed = new ConsumerRecord<byte[], byte[]>(inputTopic, 0, 0, record.key(), "invalid-json".getBytes(StandardCharsets.UTF_8));
        record.headers().forEach(h -> malformed.headers().add(h)); input(malformed);
        String group = topic + ".independent"; var tp = new TopicPartition(inputTopic, 0);
        try {
            try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]>(Map.of("bootstrap.servers", bootstrap,
                    "group.id", group, "enable.auto.commit", false), new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
                consumer.assign(List.of(tp)); consumer.commitSync(Map.of(tp, new org.apache.kafka.clients.consumer.OffsetAndMetadata(1)));
            }
            var first = intakeWorker(planner(NOW)).runOnce(0, 2);
            assertEquals(2, first.archived()); assertEquals(2, first.nextOffset()); assertEquals(1, records());
            var second = intakeWorker(planner(NOW)).runOnce(999, 2); // persisted cursor wins over bootstrap argument
            assertEquals(3, second.nextOffset()); assertEquals(1, second.backlog().held());
            assertNotNull(second.backlog().oldestHeldAt()); assertEquals(0, second.backlog().unprocessed());
            assertEquals(3, count("dlt_intake_record WHERE cluster_alias='" + topic + "'"));
            assertFalse(MAPPER.writeValueAsString(second).contains("private-payload"));
            assertEquals(0, intakeWorker(planner(NOW)).runOnce(0, 2).archived()); assertEquals(1, records());
            assertEquals(1, admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(tp).offset());
        } finally { admin.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS); }
    }

    @Test void archiveAndCursorRollBackTogetherAndStaleArchiveDoesNotDuplicate() throws Exception {
        createInput(); input(record);
        var intake = intakeStore(); var scope = intakeScope(); intake.cursor(scope, 0);
        var page = DltInspector.readRaw(bootstrap, inputTopic, 0, 0, 10);
        var bad = new ConsumerRecord<byte[], byte[]>("different-topic", 0, 1, record.key(), record.value());
        var invalid = new DltInspector.RawPage(0, 2, 2, List.of(page.records().getFirst(), bad));
        assertThrows(IllegalArgumentException.class, () -> intake.archive(scope, 0, invalid));
        assertEquals(0, intake.cursor(scope, 0));
        assertEquals(0, count("dlt_intake_record WHERE cluster_alias='" + topic + "'"));
        assertTrue(intake.archive(scope, 0, page)); assertFalse(intake.archive(scope, 0, page));
        assertEquals(1, intake.cursor(scope, 0));
        assertEquals(1, count("dlt_intake_record WHERE cluster_alias='" + topic + "'"));
    }

    @Test void archivedUnregisteredWorkSurvivesKafkaRetentionAndPreservesExpiry() throws Exception {
        createInput(); input(record); removeOrigin(); enableRestore(NOW.minusSeconds(1));
        var intake = intakeStore(); intake.cursor(intakeScope(), 0);
        assertTrue(intake.archive(intakeScope(), 0, DltInspector.readRaw(bootstrap, inputTopic, 0, 0, 10)));
        admin.deleteRecords(Map.of(new TopicPartition(inputTopic, 0), RecordsToDelete.beforeOffset(1))).all().get(10, TimeUnit.SECONDS);
        var cycle = intakeWorker(planner(NOW.plus(Duration.ofHours(8)))).runOnce(0, 10);
        assertEquals(0, cycle.archived()); assertEquals(0, cycle.backlog().unprocessed()); assertEquals(1, records());
        var command = MAPPER.readValue(DltInspector.fetchExact(bootstrap, topic, 0, 0).value(), DeliveryEvent.class);
        assertEquals(NOW, command.occurredAt()); assertFalse(command.fallbackAllowed());
        assertEquals(1, count("dlt_intake_record WHERE cluster_alias='" + topic + "' AND state='REGISTERED' AND operation_id IS NOT NULL"));
    }

    @Test void retentionGapStopsWithoutResettingCursorOrSending() throws Exception {
        createInput(); input(record); input(record);
        intakeStore().cursor(intakeScope(), 0);
        admin.deleteRecords(Map.of(new TopicPartition(inputTopic, 0), RecordsToDelete.beforeOffset(1))).all().get(10, TimeUnit.SECONDS);
        var cycle = intakeWorker(planner(NOW)).runOnce(0, 10);
        assertEquals("RETENTION_GAP", cycle.status()); assertEquals(1, cycle.beginningOffset()); assertEquals(0, cycle.nextOffset());
        assertEquals(0, intakeStore().cursor(intakeScope(), 999)); assertEquals(0, records());
        assertEquals(0, count("dlt_intake_record WHERE cluster_alias='" + topic + "'"));
    }

    @Test void unavailableHistoryKeepsArchivedWorkNewThenRecovers() throws Exception {
        createInput(); input(record);
        var failing = new DltRecoveryPlanner(db, (tenant, key, id) -> { throw new IllegalStateException("SQL unavailable"); },
                Duration.ofHours(3), "source", Clock.fixed(NOW, ZoneOffset.UTC));
        var first = intakeWorker(failing).runOnce(0, 10);
        assertEquals(1, first.nextOffset()); assertEquals(1, first.backlog().unprocessed()); assertEquals(0, records());
        var recovered = intakeWorker(planner(NOW)).runOnce(0, 10);
        assertEquals(0, recovered.backlog().unprocessed()); assertEquals(1, records());
    }

    @Test void completedHistoryIsArchivedWithHoldReasonWithoutReplay() throws Exception {
        createInput(); input(record); history(); removeOrigin();
        var cycle = intakeWorker(planner(NOW)).runOnce(0, 10);
        assertEquals(1, cycle.backlog().held()); assertEquals("HISTORY_FOUND", cycle.outcomes().getFirst().decision());
        assertEquals(0, records()); assertTrue(origin().isEmpty());
        assertEquals(0, intakeWorker(planner(NOW)).runOnce(0, 10).outcomes().size());
    }

    @Test void existingCursorRejectsChangedRoutingAndAfterEndNeverAdvances() throws Exception {
        createInput(); intakeStore().cursor(intakeScope(), 5);
        var changed = new DltIntakeStore.Scope(topic, inputTopic, 0, "another-source", topic);
        assertThrows(IllegalStateException.class, () -> intakeStore().cursor(changed, 0));
        var cycle = intakeWorker(planner(NOW)).runOnce(0, 10);
        assertEquals("OFFSET_AFTER_END", cycle.status()); assertEquals(5, cycle.nextOffset()); assertEquals(0, records());
    }

    @Test void concurrentIntakeClassificationUsesSingleDurableOutcome() throws Exception {
        createInput(); input(record); var intake = intakeStore(); intake.cursor(intakeScope(), 0);
        intake.archive(intakeScope(), 0, DltInspector.readRaw(bootstrap, inputTopic, 0, 0, 10));
        var id = intake.pending(intakeScope(), 10).getFirst();
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var actions = new AtomicInteger();
        try (var executor = Executors.newSingleThreadExecutor()) {
            var first = executor.submit(() -> intake.process(id, raw -> {
                actions.incrementAndGet(); entered.countDown();
                try { assertTrue(release.await(10, TimeUnit.SECONDS)); } catch (InterruptedException e) { throw new RuntimeException(e); }
                return new DltIntakeStore.Outcome("HELD", "MANUAL_REVIEW", null);
            }));
            try {
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                assertEquals("BUSY", intake.process(id, raw -> { fail("Must not run concurrently"); return null; }).decision());
            } finally { release.countDown(); }
            assertEquals("HELD", first.get(10, TimeUnit.SECONDS).state());
            assertEquals("HELD", intake.process(id, raw -> { fail("Must not repeat held action"); return null; }).state());
            assertEquals(1, actions.get());
        }
    }

    DltRecoveryResumer resumer(Instant now) {
        return new DltRecoveryResumer(store, planner(now), topic, topic, "source", this::publish);
    }
    void agePending(UUID operation) throws Exception {
        try (var connection = connect(); var update = connection.prepareStatement("UPDATE " + schema
                + ".dlt_recovery_operation SET updated_at=clock_timestamp()-interval '2 minutes' WHERE operation_id=?")) {
            update.setObject(1, operation); update.executeUpdate();
        }
    }
    DltRecoveryStore.Result submitPending(DltRecoveryPlanner.Plan plan) {
        return store.apply(topic, topic, plan, "tester", "submit recovery", () -> { throw new IllegalStateException("SQL down"); },
                this::publish, DltRecoveryCheckpoint.capture(plan, record));
    }

    @Test void sqlConnectionOutageDefersRepeatedScansAndResumesStoredCommand() throws Exception {
        var plan = planner(NOW).plan(record); var pending = submitPending(plan); agePending(pending.operationId());
        var down = new java.util.concurrent.atomic.AtomicBoolean(true); var connections = new AtomicInteger();
        var flaky = new DltRecoveryStore(() -> {
            connections.incrementAndGet();
            if (down.get()) throw new SQLException("unavailable", "08006");
            return connect();
        }, schema);
        var nanos = new java.util.concurrent.atomic.AtomicLong();
        var gate = new event.common.recovery.FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30), nanos::get, () -> 1.0);
        var resumer = new DltRecoveryResumer(flaky, planner(NOW.plus(Duration.ofHours(8))), topic, topic, "source", this::publish, gate);
        assertThrows(IllegalStateException.class, () -> resumer.runOnce(10, Duration.ofSeconds(30)));
        for (int i = 0; i < 100; i++) assertEquals("BACKOFF", resumer.runOnce(10, Duration.ofSeconds(30)).status());
        assertEquals(1, connections.get()); assertEquals(0, records());
        down.set(false); nanos.addAndGet(Duration.ofSeconds(1).toNanos());
        var recovered = resumer.runOnce(10, Duration.ofSeconds(30));
        assertEquals("SCANNED", recovered.status()); assertEquals(1, recovered.selected());
        assertEquals("ACKNOWLEDGED", recovered.results().getFirst().status());
        assertEquals(plan.command(), MAPPER.readValue(DltInspector.fetchExact(bootstrap, topic, 0, 0).value(), DeliveryEvent.class));
        assertEquals(1, count("dlt_recovery_operation WHERE cluster_alias='" + topic + "'"));
    }

    @Test void returnedBackendFailureStopsBatchAndProbeSelectsOnlyOnePendingOperation() throws Exception {
        var first = submitPending(planner(NOW).plan(record));
        var other = new ConsumerRecord<>("dlt", 0, 6, record.key(), record.value());
        record.headers().forEach(h -> other.headers().add(h));
        other.headers().remove(org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_OFFSET);
        other.headers().add(org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_OFFSET, java.nio.ByteBuffer.allocate(8).putLong(43).array());
        var plan = planner(NOW).plan(other);
        var second = store.apply(topic, topic, plan, "tester", "submit second recovery", () -> { throw new IllegalStateException(); },
                this::publish, DltRecoveryCheckpoint.capture(plan, other));
        agePending(first.operationId()); agePending(second.operationId());
        var down = new java.util.concurrent.atomic.AtomicBoolean(true); var calls = new AtomicInteger();
        var nanos = new java.util.concurrent.atomic.AtomicLong();
        var gate = new event.common.recovery.FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30), nanos::get, () -> 1.0);
        var resumer = new DltRecoveryResumer(store, planner(NOW), topic, topic, "source", command -> {
            calls.incrementAndGet(); if (down.get()) throw new TimeoutException("Kafka acknowledgement unavailable");
            return publish(command);
        }, gate);
        var failed = resumer.runOnce(10, Duration.ofSeconds(30));
        assertEquals("BACKEND_UNAVAILABLE", failed.status()); assertEquals(2, failed.selected()); assertEquals(1, failed.results().size());
        assertTrue(failed.results().getFirst().backendUnavailable()); assertEquals(1, calls.get());
        long attempts = count("dlt_recovery_attempt WHERE operation_id IN ('" + first.operationId() + "','" + second.operationId() + "')");
        for (int i = 0; i < 100; i++) assertEquals("BACKOFF", resumer.runOnce(10, Duration.ofSeconds(30)).status());
        assertEquals(attempts, count("dlt_recovery_attempt WHERE operation_id IN ('" + first.operationId() + "','" + second.operationId() + "')"));
        down.set(false); agePending(first.operationId()); agePending(second.operationId()); nanos.addAndGet(Duration.ofSeconds(1).toNanos());
        var probe = resumer.runOnce(10, Duration.ofSeconds(30));
        assertEquals(1, probe.selected()); assertEquals("ACKNOWLEDGED", probe.results().getFirst().status());
        assertEquals("ACKNOWLEDGED", resumer.runOnce(10, Duration.ofSeconds(30)).results().getFirst().status());
        assertEquals(2, records());
        assertArrayEquals(DltInspector.fetchExact(bootstrap, topic, 0, 0).value(), DltInspector.fetchExact(bootstrap, topic, 0, 1).value());
    }

    @Test void intakeBackoffRetainsRawArchiveAndCursorThenResumesBothRecords() throws Exception {
        createInput(); input(record);
        var other = new ConsumerRecord<>("dlt", 0, 6, record.key(), record.value());
        record.headers().forEach(h -> other.headers().add(h));
        other.headers().remove(org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_OFFSET);
        other.headers().add(org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_OFFSET, java.nio.ByteBuffer.allocate(8).putLong(43).array());
        input(other);
        var down = new java.util.concurrent.atomic.AtomicBoolean(true); var reads = new AtomicInteger();
        var nanos = new java.util.concurrent.atomic.AtomicLong();
        var gate = new event.common.recovery.FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30), nanos::get, () -> 1.0);
        var worker = new DltIntakeWorker(intakeStore(), store, planner(NOW), intakeScope(), (offset, limit) -> {
            reads.incrementAndGet(); return DltInspector.readRaw(bootstrap, inputTopic, 0, offset, limit);
        }, command -> { if (down.get()) throw new TimeoutException("Kafka acknowledgement unavailable"); return publish(command); }, gate);
        var failed = worker.runOnce(0, 10);
        assertEquals("BACKEND_UNAVAILABLE", failed.status()); assertEquals(2, failed.archived()); assertEquals(2, failed.nextOffset());
        assertEquals(1, failed.outcomes().size()); assertTrue(failed.outcomes().getFirst().backendUnavailable()); assertNull(failed.backlog());
        var operation = failed.outcomes().getFirst().operationId(); assertNotNull(operation);
        for (int i = 0; i < 100; i++) {
            var skipped = worker.runOnce(0, 10); assertEquals("BACKOFF", skipped.status());
            assertEquals(-1, skipped.nextOffset()); assertNull(skipped.backlog());
        }
        assertEquals(1, reads.get()); assertEquals(2, intakeStore().cursor(intakeScope(), 0));
        assertEquals(2, count("dlt_intake_record WHERE cluster_alias='" + topic + "'"));
        assertEquals(1, count("dlt_recovery_attempt WHERE operation_id='" + operation + "'"));
        down.set(false); nanos.addAndGet(Duration.ofSeconds(1).toNanos());
        var recovered = worker.runOnce(999, 10);
        assertEquals("SCANNED", recovered.status()); assertEquals(2, recovered.nextOffset());
        assertEquals("ACKNOWLEDGED", recovered.outcomes().getFirst().decision()); assertEquals(1, records());
        agePending(operation);
        assertEquals("ACKNOWLEDGED", resumer(NOW.plus(Duration.ofHours(8))).runOnce(10, Duration.ofSeconds(30)).results().getFirst().status());
        assertEquals(2, records());
        assertEquals(execution.toDispatchRequested(), MAPPER.readValue(DltInspector.fetchExact(bootstrap, topic, 0, 1).value(), DeliveryEvent.class));
    }

    @Test void autoResumeRecoversPersistedStartedWithoutReadingKafkaSource() throws Exception {
        removeOrigin(); enableRestore(NOW.minusSeconds(1));
        var plan = planner(NOW).plan(record);
        // Model abrupt termination after STARTED commit; no exception recovery/finish call is allowed.
        assertThrows(AssertionError.class, () -> store.apply(topic, topic, plan, "tester", "crash boundary",
                () -> { throw new AssertionError("termination model"); }, this::publish, DltRecoveryCheckpoint.capture(plan, record)));
        assertEquals(1, count("dlt_recovery_operation WHERE cluster_alias='" + topic + "' AND state='PENDING'"));
        UUID operation;
        try (var connection = connect(); var query = connection.prepareStatement("SELECT operation_id FROM " + schema
                + ".dlt_recovery_operation WHERE cluster_alias=?")) {
            query.setString(1, topic);
            try (var row = query.executeQuery()) { assertTrue(row.next()); operation = row.getObject(1, UUID.class); }
        }
        agePending(operation);
        var cycle = resumer(NOW.plus(Duration.ofHours(8))).runOnce(10, Duration.ofSeconds(30));
        assertEquals(1, cycle.selected()); assertEquals("ACKNOWLEDGED", cycle.results().getFirst().status());
        var received = MAPPER.readValue(DltInspector.fetchExact(bootstrap, topic, 0, 0).value(), DeliveryEvent.class);
        assertEquals(plan.command(), received); assertEquals(NOW, received.occurredAt());
        assertEquals(1, count("dlt_recovery_attempt WHERE operation_id='" + operation + "' AND actor='dlt-auto-resumer' AND outcome='ACKNOWLEDGED'"));
        assertEquals(0, resumer(NOW).runOnce(10, Duration.ofSeconds(30)).selected());
    }

    @Test void autoResumeReleasesInterruptedHoldUsingSameExecution() throws Exception {
        removeOrigin(); enableRestore(NOW.minusSeconds(1));
        var plan = planner(NOW).plan(record);
        var result = store.apply(topic, topic, plan, "tester", "interrupt after reserve", () -> {
            new DeliveryRepository(db, MAPPER).reserveRecovery(admitted.execution(plan.command().deliveryId()), plan.command().deliveryId());
            throw new IllegalStateException("history unavailable");
        }, this::publish, DltRecoveryCheckpoint.capture(plan, record));
        assertTrue(origin().containsKey(DeliveryCompletion.RECOVERY_HOLD));
        agePending(result.operationId());
        assertEquals("ACKNOWLEDGED", resumer(NOW).runOnce(10, Duration.ofSeconds(30)).results().getFirst().status());
        assertEquals(plan.command().deliveryId(), origin().get("delivery_id").s());
        assertFalse(origin().containsKey(DeliveryCompletion.RECOVERY_HOLD)); assertEquals(1, records());
    }

    @Test void automaticAckLossRetryPublishesIdenticalCommandAndThenStops() throws Exception {
        var plan = planner(NOW).plan(record);
        var result = store.apply(topic, topic, plan, "tester", "ack lost", () -> planner(NOW).plan(record), command -> {
            publish(command); throw new TimeoutException();
        }, DltRecoveryCheckpoint.capture(plan, record));
        assertEquals(0, resumer(NOW).runOnce(10, Duration.ofSeconds(30)).selected());
        agePending(result.operationId());
        assertEquals("ACKNOWLEDGED", resumer(NOW).runOnce(10, Duration.ofSeconds(30)).results().getFirst().status());
        assertEquals(2, records());
        assertArrayEquals(DltInspector.fetchExact(bootstrap, topic, 0, 0).value(), DltInspector.fetchExact(bootstrap, topic, 0, 1).value());
        assertEquals(0, resumer(NOW).runOnce(10, Duration.ofSeconds(30)).selected());
    }

    @Test void completedExecutionIsHeldAndNotAutomaticallyRetried() throws Exception {
        var result = submitPending(planner(NOW).plan(record)); agePending(result.operationId());
        history(); removeOrigin();
        assertEquals("HELD_STATE_CHANGED", resumer(NOW).runOnce(10, Duration.ofSeconds(30)).results().getFirst().status());
        assertEquals(0, records()); assertTrue(origin().isEmpty());
        assertEquals(0, resumer(NOW).runOnce(10, Duration.ofSeconds(30)).selected());
    }

    @Test void changedGenerationCannotBeActivatedBeforeStoredCommandComparison() throws Exception {
        var result = submitPending(planner(NOW).plan(record)); agePending(result.operationId());
        removeOrigin(); enableRestore(NOW.minusSeconds(1));
        // Even if absent history now permits a new reservation, the old operation authorizes a different execution.
        assertEquals("HELD_STATE_CHANGED", resumer(NOW).runOnce(10, Duration.ofSeconds(30)).results().getFirst().status());
        assertTrue(origin().isEmpty()); assertEquals(0, records());
    }

    @Test void pendingSelectionIsScopedBoundedAndLeavesLegacyOperationsManual() throws Exception {
        var plan = planner(NOW).plan(record);
        var result = submitPending(plan); agePending(result.operationId());
        assertEquals(1, store.pending(topic, topic, 1, Duration.ofSeconds(30)).size());
        assertTrue(store.pending("other", topic, 1, Duration.ofSeconds(30)).isEmpty());
        assertTrue(store.pending(topic, "other", 1, Duration.ofSeconds(30)).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> store.pending(topic, topic, 101, Duration.ofSeconds(30)));
        assertThrows(IllegalArgumentException.class, () -> store.pending(topic, topic, 1, Duration.ZERO));
        var legacy = store.apply("legacy-" + topic, topic, plan, "tester", "old operation",
                () -> { throw new IllegalStateException(); }, this::publish);
        agePending(legacy.operationId());
        assertTrue(store.pending("legacy-" + topic, topic, 1, Duration.ofSeconds(30)).isEmpty());
    }

    @Test void autoResumeUsesManualLockAndRejectsStaleSelection() throws Exception {
        var plan = planner(NOW).plan(record); var result = submitPending(plan); agePending(result.operationId());
        var candidate = store.pending(topic, topic, 1, Duration.ofSeconds(30)).getFirst();
        var checkpoint = MAPPER.readValue(candidate.checkpointJson(), DltRecoveryCheckpoint.class);
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var manual = executor.submit(() -> store.apply(topic, topic, plan, "tester", "manual retry", () -> {
                entered.countDown();
                try { assertTrue(release.await(10, TimeUnit.SECONDS)); }
                catch (InterruptedException e) { throw new RuntimeException(e); }
                throw new IllegalStateException("still unavailable");
            }, this::publish));
            try {
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                assertEquals("BUSY", store.resume(topic, topic, candidate, checkpoint, () -> planner(NOW).prepare(record), this::publish).status());
            } finally { release.countDown(); }
            assertEquals("UNCONFIRMED", manual.get(10, TimeUnit.SECONDS).status());
        }
        assertEquals("SKIPPED_CHANGED", store.resume(topic, topic, candidate, checkpoint, () -> planner(NOW).prepare(record), this::publish).status());
        assertEquals(0, records());
    }

    @Test void checkpointRejectsChangedInputBeforeOperationWrite() throws Exception {
        var plan = planner(NOW).plan(record);
        var changed = DltInspectionTest.record(DeliveryEvent.requested(UUID.randomUUID().toString(), 999L, "SMS", Map.of("text", "changed"), NOW).forAdmission());
        assertThrows(IllegalArgumentException.class, () -> DltRecoveryCheckpoint.capture(plan, changed));
        assertEquals(0, count("dlt_recovery_operation WHERE cluster_alias='" + topic + "'"));
        assertEquals(0, records());
    }

    @Test void coverageIsRequiredAndNeverBackdatesItselfForOldRequests() throws Exception {
        removeOrigin();
        assertEquals(NO_ORIGIN_UNCONFIRMED, planner(NOW).plan(record).preview().decision());
        enableRestore(NOW.plusSeconds(1));
        assertEquals(NO_ORIGIN_UNCONFIRMED, planner(NOW).plan(record).preview().decision());
        enableRestore(NOW);
        assertEquals(RESTORE_ORIGIN, planner(NOW).plan(record).preview().decision());
        assertTrue(origin().isEmpty()); // planning remains read-only
    }

    @Test void initialFailureRestoresOnceAndRetainsOccurrenceThroughKafka() throws Exception {
        removeOrigin(); enableRestore(NOW.minusSeconds(1));
        var planner = planner(NOW); var plan = planner.plan(record);
        assertEquals(RESTORE_ORIGIN, plan.preview().decision());
        assertNotEquals(execution.deliveryId(), plan.command().deliveryId());
        var result = store.apply(topic, topic, plan, "tester", "restore initial failure", () -> planner.prepare(record), this::publish);
        assertEquals("ACKNOWLEDGED", result.status());
        assertEquals(plan.command().deliveryId(), origin().get("delivery_id").s());
        assertFalse(origin().containsKey(DeliveryCompletion.RECOVERY_HOLD));
        assertTrue(origin().containsKey("lifecycle_bucket"));
        assertEquals(plan.command(), MAPPER.readValue(DltInspector.fetchExact(bootstrap, topic, 0, 0).value(), DeliveryEvent.class));
        assertEquals(NOW, plan.command().occurredAt());
        assertEquals("ALREADY_ACKNOWLEDGED", store.apply(topic, topic, planner.plan(record), "tester", "retry", () -> planner.prepare(record), this::publish).status());
        assertEquals(1, records());
    }

    @Test void expiredInitialFailureKeepsOriginalDeadlineAndFallbackFlag() throws Exception {
        removeOrigin(); enableRestore(NOW.minusSeconds(1));
        var planner = planner(NOW.plus(Duration.ofHours(8)));
        var plan = planner.plan(record);
        var prepared = planner.prepare(record);
        assertEquals(EXPIRE_EXISTING, prepared.preview().decision());
        assertEquals(plan.command(), prepared.command());
        assertEquals(NOW, prepared.command().occurredAt());
        assertFalse(prepared.command().fallbackAllowed());
    }

    @Test void sqlFailureAfterReservationLeavesBlockedOriginAndSameGenerationResumes() throws Exception {
        removeOrigin(); enableRestore(NOW.minusSeconds(1));
        var reads = new AtomicInteger();
        var failing = new DltRecoveryPlanner(db, (tenant, key, id) -> {
            if (reads.incrementAndGet() == 2) throw new IllegalStateException("SQL temporarily unavailable");
            return store.read(tenant, key, id);
        }, Duration.ofHours(3), "source", Clock.fixed(NOW, ZoneOffset.UTC));
        var plan = planner(NOW).plan(record);
        var result = store.apply(topic, topic, plan, "tester", "restore", () -> failing.prepare(record), this::publish);
        assertEquals("UNCONFIRMED", result.status()); assertEquals(0, records());
        assertEquals(plan.command().deliveryId(), origin().get(DeliveryCompletion.RECOVERY_HOLD).s());
        assertFalse(origin().containsKey("lifecycle_bucket"));
        assertThrows(IllegalStateException.class, () -> new DeliveryRepository(db, MAPPER).saveOrLoad(admitted));
        var restarted = planner(NOW.plusSeconds(1));
        assertEquals(plan.command(), restarted.plan(record).command());
        assertEquals("ACKNOWLEDGED", store.apply(topic, topic, restarted.plan(record), "tester", "resume",
                () -> restarted.prepare(record), this::publish).status());
        assertEquals(1, records());
    }

    @Test void historyCommittedBetweenInitialReadAndReservationPreventsActivation() throws Exception {
        removeOrigin(); enableRestore(NOW.minusSeconds(1));
        var plan = planner(NOW).plan(record); var reads = new AtomicInteger();
        var racing = new DltRecoveryPlanner(db, (tenant, key, id) -> {
            if (reads.incrementAndGet() == 2) {
                assertTrue(origin().containsKey(DeliveryCompletion.RECOVERY_HOLD));
                try { history(); } catch (Exception failure) { throw new RuntimeException(failure); }
            }
            return store.read(tenant, key, id);
        }, Duration.ofHours(3), "source", Clock.fixed(NOW, ZoneOffset.UTC));
        var result = store.apply(topic, topic, plan, "tester", "restore", () -> racing.prepare(record), this::publish);
        assertEquals("HELD_STATE_CHANGED", result.status());
        assertTrue(origin().isEmpty()); assertEquals(0, records());
    }

    @Test void normalAdmissionWinningReservationIsNeverOverwrittenOrDeleted() throws Exception {
        removeOrigin(); enableRestore(NOW.minusSeconds(1));
        var plan = planner(NOW).plan(record);
        var normal = new DeliveryRepository(db, MAPPER).saveOrLoad(admitted).event();
        var planner = planner(NOW);
        var result = store.apply(topic, topic, plan, "tester", "restore", () -> planner.prepare(record), this::publish);
        assertEquals("HELD_STATE_CHANGED", result.status()); assertEquals(0, records());
        assertEquals(normal.deliveryId(), origin().get("delivery_id").s());
    }

    @Test void disabledCoverageCannotActivateAnInterruptedRestoration() throws Exception {
        removeOrigin(); enableRestore(NOW.minusSeconds(1));
        var plan = planner(NOW).plan(record);
        new DeliveryRepository(db, MAPPER).reserveRecovery(admitted.execution(plan.command().deliveryId()), plan.command().deliveryId());
        try (var connection = connect(); var statement = connection.createStatement()) {
            statement.execute("UPDATE " + schema + ".dlt_history_coverage SET origin_restore_enabled=false");
        }
        assertEquals(RECOVERY_HELD, planner(NOW).prepare(record).preview().decision());
        assertTrue(origin().containsKey(DeliveryCompletion.RECOVERY_HOLD));
        assertEquals(0, records());
    }

    @Test void keepsExecutionAndDeadlineIncludingExpiredRequests() {
        var plan = planner(NOW).plan(record);
        assertEquals(RESUME_EXISTING, plan.preview().decision());
        assertEquals(execution.toDispatchRequested(), plan.command());
        var expired = planner(NOW.plus(Duration.ofHours(3))).plan(record);
        assertEquals(EXPIRE_EXISTING, expired.preview().decision());
        assertEquals(plan.command(), expired.command());
        assertFalse(MAPPER.writeValueAsString(plan.preview()).contains("private-payload"));
    }

    @Test void noOriginAndNoHistoryNeverCreatesANewExecution() {
        db.deleteItem(r -> r.tableName("ORIGIN").key(DeliveryCompletion.metaKey(admitted.requestKey())));
        assertEquals(NO_ORIGIN_UNCONFIRMED, planner(NOW).plan(record).preview().decision());
        assertFalse(planner(NOW).plan(record).eligible());
    }

    @Test void sqlHistoryHoldsBeforeAndAfterOriginCleanup() throws Exception {
        history();
        assertEquals(HISTORY_FOUND, planner(NOW).plan(record).preview().decision());
        db.deleteItem(r -> r.tableName("ORIGIN").key(DeliveryCompletion.metaKey(admitted.requestKey())));
        assertEquals(HISTORY_FOUND, planner(NOW).plan(record).preview().decision());
    }

    @Test void changedOriginAndExistingStepAreHeld() {
        db.putItem(r -> r.tableName("STEP").item(stepKey()));
        assertEquals(STEP_ALREADY_TRACKED, planner(NOW).plan(record).preview().decision());
        db.deleteItem(r -> r.tableName("STEP").key(stepKey()));
        db.updateItem(r -> r.tableName("ORIGIN").key(DeliveryCompletion.metaKey(admitted.requestKey()))
                .updateExpression("SET occurred_at=:changed").expressionAttributeValues(Map.of(":changed", AttributeValue.fromS(NOW.plusSeconds(1).toString()))));
        assertEquals(ORIGIN_MISMATCH, planner(NOW).plan(record).preview().decision());
    }

    @Test void completionFenceHoldsAndSqlFailureCannotBeTreatedAsNoHistory() {
        db.updateItem(r -> r.tableName("ORIGIN").key(DeliveryCompletion.metaKey(admitted.requestKey()))
                .updateExpression("SET completion_event_id=:id").expressionAttributeValues(Map.of(":id", AttributeValue.fromS(UUID.randomUUID().toString()))));
        assertEquals(COMPLETION_RECORDED, planner(NOW).plan(record).preview().decision());
        var unavailable = new DltRecoveryStore(() -> { throw new SQLException("unavailable"); }, schema);
        var planner = new DltRecoveryPlanner(db, unavailable, Duration.ofHours(3), "source", Clock.fixed(NOW, ZoneOffset.UTC));
        assertThrows(IllegalStateException.class, () -> planner.plan(record));
    }

    @Test void publishIsDurablyAcknowledgedAndSameOperationDoesNotPublishAgain() throws Exception {
        var planner = planner(NOW); var plan = planner.plan(record);
        var first = store.apply(topic, topic, plan, "tester", "recover", () -> planner.plan(record), this::publish);
        assertEquals("ACKNOWLEDGED", first.status());
        assertEquals("ALREADY_ACKNOWLEDGED", store.apply(topic, topic, plan, "tester", "again", () -> planner.plan(record), this::publish).status());
        assertEquals(1, records());
        assertEquals(1, count("dlt_recovery_attempt WHERE operation_id='" + first.operationId() + "' AND outcome='ACKNOWLEDGED'"));
        var received = DltInspector.fetchExact(bootstrap, topic, 0, 0);
        assertEquals(execution.deliveryId(), new String(received.key(), StandardCharsets.UTF_8));
        assertEquals(plan.command(), MAPPER.readValue(received.value(), DeliveryEvent.class));
    }

    @Test void lostKafkaAcknowledgementRetainsSameCommandForRetry() throws Exception {
        var planner = planner(NOW); var plan = planner.plan(record);
        var first = store.apply(topic, topic, plan, "tester", "recover", () -> planner.plan(record), command -> {
            publish(command); throw new TimeoutException("broker ack lost");
        });
        assertEquals("UNCONFIRMED", first.status());
        assertEquals(1, count("dlt_recovery_operation WHERE operation_id='" + first.operationId() + "' AND state='PENDING'"));
        assertEquals("ACKNOWLEDGED", store.apply(topic, topic, plan, "tester", "retry", () -> planner.plan(record), this::publish).status());
        assertEquals(2, records());
        assertArrayEquals(DltInspector.fetchExact(bootstrap, topic, 0, 0).value(), DltInspector.fetchExact(bootstrap, topic, 0, 1).value());
        assertEquals(1, count("dlt_recovery_attempt WHERE operation_id='" + first.operationId() + "' AND outcome='UNCONFIRMED'"));
        assertEquals(2, count("dlt_recovery_attempt WHERE operation_id='" + first.operationId() + "'"));
    }

    @Test void changedStateAfterPlanningPreventsPublishAndLeavesAudit() throws Exception {
        var planner = planner(NOW); var plan = planner.plan(record);
        db.deleteItem(r -> r.tableName("ORIGIN").key(DeliveryCompletion.metaKey(admitted.requestKey())));
        var result = store.apply(topic, topic, plan, "tester", "recover", () -> planner.plan(record), this::publish);
        assertEquals("HELD_STATE_CHANGED", result.status()); assertEquals(0, records());
        assertEquals(1, count("dlt_recovery_attempt WHERE operation_id='" + result.operationId() + "' AND outcome='STATE_CHANGED'"));
    }

    @Test void concurrentOperatorsSerializeBeforePublishing() throws Exception {
        var planner = planner(NOW); var plan = planner.plan(record);
        var started = new CountDownLatch(1); var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        try (var executor = Executors.newSingleThreadExecutor()) {
            var first = executor.submit(() -> store.apply(topic, topic, plan, "tester", "recover", () -> planner.plan(record), command -> {
                calls.incrementAndGet(); started.countDown();
                assertTrue(release.await(10, TimeUnit.SECONDS)); return publish(command);
            }));
            try {
                assertTrue(started.await(10, TimeUnit.SECONDS));
                assertEquals("BUSY", store.apply(topic, topic, plan, "tester2", "recover", () -> planner.plan(record), this::publish).status());
            } finally { release.countDown(); }
            assertEquals("ACKNOWLEDGED", first.get(10, TimeUnit.SECONDS).status());
            assertEquals(1, calls.get()); assertEquals(1, records());
        }
    }

    @Test void originalCoordinatesCannotBeReusedWithAnotherCommand() throws Exception {
        var planner = planner(NOW); var plan = planner.plan(record);
        var first = store.apply(topic, topic, plan, "tester", "recover", () -> planner.plan(record), this::publish);
        var forged = new DltRecoveryPlanner.Plan(plan.preview(), plan.command().execution(UUID.randomUUID().toString()));
        assertThrows(IllegalStateException.class, () -> store.apply(topic, topic, forged, "tester", "changed", () -> forged, this::publish));
        assertEquals(1, records());
        assertEquals(1, count("dlt_recovery_operation WHERE operation_id='" + first.operationId() + "' AND state='ACKNOWLEDGED'"));
    }
}

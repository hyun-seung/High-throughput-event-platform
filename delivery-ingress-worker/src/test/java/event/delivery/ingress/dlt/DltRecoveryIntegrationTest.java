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
                    "V3__delivery_cleanup_reservations.sql", "V4__dlt_recovery_handoff.sql")) {
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
            try { admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS); } finally { admin.close(Duration.ofSeconds(5)); }
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

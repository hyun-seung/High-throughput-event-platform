package event.delivery.result;

import com.zaxxer.hikari.*;
import event.common.delivery.*;
import event.common.lifecycle.*;
import event.delivery.result.cleanup.*;
import event.delivery.result.notification.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.SpringApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import tools.jackson.databind.json.JsonMapper;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static event.common.dynamodb.DynamoDbTableNames.DELIVERY_STATE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@EnabledIfEnvironmentVariable(named = "POSTGRES_TEST_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "DYNAMODB_TEST_ENDPOINT", matches = ".+")
class CleanupPostgresDynamoDbTest {
    final String schema = "cleanup_test_" + UUID.randomUUID().toString().replace("-", "");
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    final List<DeliveryFinalized> results = new ArrayList<>();
    HikariDataSource pool;
    JdbcTemplate jdbc;
    DynamoDbClient db;
    FinalizedStore store;
    CleanupRepository repository;
    String url, user, password;

    @BeforeEach void setup() {
        url = System.getenv("POSTGRES_TEST_URL"); var endpoint = URI.create(System.getenv("DYNAMODB_TEST_ENDPOINT"));
        if (!Set.of("localhost", "127.0.0.1").contains(URI.create(url.substring(5)).getHost())
                || !Set.of("localhost", "127.0.0.1").contains(endpoint.getHost())) throw new IllegalArgumentException("Local test endpoints required");
        user = System.getenv().getOrDefault("POSTGRES_TEST_USER", "delivery"); password = System.getenv().getOrDefault("POSTGRES_TEST_PASSWORD", "delivery");
        Flyway.configure().dataSource(url, user, password).defaultSchema(schema).schemas(schema).load().migrate();
        var config = new HikariConfig(); config.setJdbcUrl(url); config.setUsername(user); config.setPassword(password); config.setSchema(schema); config.setMaximumPoolSize(8);
        pool = new HikariDataSource(config); jdbc = new JdbcTemplate(pool);
        db = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(10))).build();
        store = new FinalizedStore(jdbc, new DataSourceTransactionManager(pool), new FinalizedCodec(), meters);
        repository = new CleanupRepository(jdbc, new DataSourceTransactionManager(pool));
    }
    @AfterEach void cleanup() {
        if (db != null) {
            for (var e : results) for (String sk : List.of("META", "FINAL", "ATTEMPT#" + e.attemptId()))
                db.deleteItem(r -> r.tableName(DELIVERY_STATE).key(key(e, sk)));
            db.close();
        }
        if (pool != null) { try { jdbc.execute("DROP SCHEMA " + schema + " CASCADE"); } finally { pool.close(); } }
        meters.close();
    }

    DeliveryFinalized seed() {
        var e = FinalizedCodecTest.event("DELIVERED", 1); results.add(e);
        var meta = new HashMap<>(key(e, "META"));
        meta.put("delivery_id", s(e.deliveryId())); meta.put("event_id", s(DeliveryIds.eventId(e.deliveryId(), DeliveryEventType.DELIVERY_REQUESTED)));
        meta.put("tenant_id", AttributeValue.fromN("42")); meta.put("delivery_type", s(e.deliveryType()));
        meta.put("occurred_at", s(e.occurredAt().toString())); meta.put("payload", s("{\"body\":\"" + "x".repeat(64000) + "\"}"));
        meta.put("status", s("ACCEPTED")); meta.put(DeliveryCompletion.FENCE, s(e.eventId()));
        db.putItem(r -> r.tableName(DELIVERY_STATE).item(meta));
        var attempt = new HashMap<>(key(e, "ATTEMPT#" + e.attemptId()));
        attempt.put("attempt_id", s(e.attemptId())); attempt.put("version", AttributeValue.fromN("2"));
        attempt.put("lifecycle_closed", AttributeValue.fromBool(true)); attempt.put(DeliveryCompletion.TRACKING_VERSION, AttributeValue.fromN("1"));
        db.putItem(r -> r.tableName(DELIVERY_STATE).item(attempt));
        var completed = new HashMap<>(key(e, "FINAL")); completed.put("result_event", s(new FinalizedCodec().encode(e))); completed.put("publish_state", s("PUBLISHED"));
        db.putItem(r -> r.tableName(DELIVERY_STATE).item(completed)); return e;
    }
    Map<String, AttributeValue> meta(DeliveryFinalized e) { return db.getItem(r -> r.tableName(DELIVERY_STATE).key(key(e, "META")).consistentRead(true)).item(); }
    static Map<String, AttributeValue> key(DeliveryFinalized e, String sk) { return Map.of("pk", s("DELIVERY#" + e.deliveryId()), "sk", s(sk)); }
    static AttributeValue s(String value) { return AttributeValue.fromS(value); }
    CleanupWorker worker(CleanupRepository repository) { return new CleanupWorker(repository, new DeliveryCompactor(db), meters); }

    @Test void noSqlEvidenceOrMissingNotificationCannotDeleteOriginal() {
        var e = seed(); worker(repository).tick();
        assertTrue(meta(e).containsKey("payload"));
        store.save(e); jdbc.update("DELETE FROM customer_notification_outbox WHERE result_event_id = ?", UUID.fromString(e.eventId()));
        worker(repository).tick(); assertTrue(meta(e).containsKey("payload"));
        assertEquals("PENDING", jdbc.queryForObject("SELECT status FROM delivery_cleanup_outbox", String.class));
    }

    @Test void cleanupReservationFailureRollsBackHistoryAndNotificationTogether() {
        var e = seed();
        jdbc.execute("ALTER TABLE delivery_cleanup_outbox ADD CONSTRAINT injected_failure CHECK (status = 'DONE')");
        assertThrows(org.springframework.dao.DataAccessException.class, () -> store.save(e));
        for (String table : List.of("delivery_history", "customer_notification_outbox", "delivery_cleanup_outbox"))
            assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM " + table, Integer.class));
        assertTrue(meta(e).containsKey("payload"));
    }

    @Test void compactedOriginalStillAllowsPendingCustomerHttpAndDuplicateFinalDoesNotResetState() throws Exception {
        var e = seed(); store.save(e); worker(repository).tick();
        assertTrue(DeliveryCompletion.compacted(meta(e))); assertFalse(meta(e).containsKey("payload"));
        assertEquals("DONE", jdbc.queryForObject("SELECT status FROM delivery_cleanup_outbox", String.class));
        assertEquals("PENDING", jdbc.queryForObject("SELECT status FROM customer_notification_outbox", String.class));
        try (var server = new NotificationTestServer(); var http = new CustomerNotificationClient(Duration.ofSeconds(1))) {
            var settings = new NotificationProperties(true, 2, 100, Duration.ofSeconds(1), Duration.ofSeconds(10), Duration.ofSeconds(1), Duration.ofSeconds(60), 262144,
                    Map.of(42L, new NotificationProperties.Destination(server.url(), NotificationTestServer.TOKEN)));
            new NotificationService(new NotificationRepository(jdbc, new DataSourceTransactionManager(pool)), http, settings, meters).deliver(42);
            assertEquals(1, server.requests.size()); assertEquals(e.eventId(), server.requests.getFirst().batch().results().getFirst().eventId());
        }
        store.save(e); worker(repository).tick();
        assertEquals("DELIVERED", jdbc.queryForObject("SELECT status FROM customer_notification_outbox", String.class));
        assertEquals(1, jdbc.queryForObject("SELECT attempt_count FROM customer_notification_outbox", Integer.class));
        assertEquals("DONE", jdbc.queryForObject("SELECT status FROM delivery_cleanup_outbox", String.class));
    }

    @Test void dynamoSuccessBeforeSqlDoneFailureRecoversFromSmallCompletionWithoutDeletingAgain() {
        var e = seed(); store.save(e);
        var failing = spy(repository); doThrow(new IllegalStateException("SQL done response unavailable")).when(failing).done(any());
        worker(failing).tick();
        assertTrue(DeliveryCompletion.compacted(meta(e)));
        assertEquals("PENDING", jdbc.queryForObject("SELECT status FROM delivery_cleanup_outbox", String.class));
        jdbc.update("UPDATE delivery_cleanup_outbox SET next_attempt_at = clock_timestamp() - interval '1 second'");
        worker(new CleanupRepository(jdbc, new DataSourceTransactionManager(pool))).tick();
        assertEquals("DONE", jdbc.queryForObject("SELECT status FROM delivery_cleanup_outbox", String.class));
        assertEquals(1, meters.counter("delivery.cleanup.events", "outcome", "already_compacted").count());
    }

    @Test void sqlLeaseLimitsConcurrencyAndOriginalReservationRecoversAfterExpiry() throws Exception {
        var e = seed(); store.save(e); var claims = new ArrayList<CleanupRepository.Claim>();
        try (var threads = Executors.newFixedThreadPool(6)) {
            List<Future<Optional<CleanupRepository.Claim>>> jobs = new ArrayList<>();
            for (int i = 0; i < 12; i++) jobs.add(threads.submit(repository::claim));
            for (var job : jobs) job.get(10, TimeUnit.SECONDS).ifPresent(claims::add);
        }
        assertEquals(1, claims.size());
        jdbc.update("UPDATE delivery_cleanup_outbox SET lease_until = clock_timestamp() - interval '1 second'");
        worker(new CleanupRepository(jdbc, new DataSourceTransactionManager(pool))).tick();
        assertEquals("DONE", jdbc.queryForObject("SELECT status FROM delivery_cleanup_outbox", String.class));
        repository.retry(claims.getFirst());
        assertEquals("DONE", jdbc.queryForObject("SELECT status FROM delivery_cleanup_outbox", String.class));
    }

    @Test void actualApplicationRunsCleanupOnDedicatedScheduler() throws Exception {
        var e = seed(); store.save(e);
        try (var app = SpringApplication.run(DeliveryResultApplication.class,
                "--server.port=0", "--management.server.port=0", "--spring.kafka.listener.auto-startup=false",
                "--spring.datasource.url=" + url, "--spring.datasource.username=" + user, "--spring.datasource.password=" + password,
                "--spring.datasource.hikari.schema=" + schema, "--spring.flyway.default-schema=" + schema, "--spring.flyway.schemas=" + schema,
                "--cleanup.enabled=true", "--cleanup.poll-ms=20", "--delivery.dynamodb.enabled=true", "--delivery.dynamodb.endpoint=" + System.getenv("DYNAMODB_TEST_ENDPOINT"))) {
            long end = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (!"DONE".equals(jdbc.queryForObject("SELECT status FROM delivery_cleanup_outbox", String.class)) && System.nanoTime() < end) Thread.sleep(20);
            assertEquals("DONE", jdbc.queryForObject("SELECT status FROM delivery_cleanup_outbox", String.class));
            assertTrue(DeliveryCompletion.compacted(meta(e))); assertTrue(app.containsBean("cleanupTaskScheduler"));
        }
    }
}

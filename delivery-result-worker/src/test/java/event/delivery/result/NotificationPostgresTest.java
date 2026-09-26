package event.delivery.result;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import event.common.lifecycle.DeliveryFinalized;
import event.delivery.result.notification.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "POSTGRES_TEST_URL", matches = ".+")
class NotificationPostgresTest {
    final String schema = "notify_test_" + UUID.randomUUID().toString().replace("-", "");
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    HikariDataSource pool;
    JdbcTemplate jdbc;
    FinalizedStore store;
    NotificationRepository repository;
    NotificationTestServer server;
    CustomerNotificationClient client;
    NotificationProperties settings;

    @BeforeEach void setup() throws Exception {
        String url = System.getenv("POSTGRES_TEST_URL");
        if (!Set.of("localhost", "127.0.0.1").contains(URI.create(url.substring(5)).getHost())) throw new IllegalArgumentException("Local PostgreSQL required");
        String user = System.getenv().getOrDefault("POSTGRES_TEST_USER", "delivery"), password = System.getenv().getOrDefault("POSTGRES_TEST_PASSWORD", "delivery");
        Flyway.configure().dataSource(url, user, password).defaultSchema(schema).schemas(schema).load().migrate();
        var config = new HikariConfig(); config.setJdbcUrl(url); config.setUsername(user); config.setPassword(password); config.setSchema(schema); config.setMaximumPoolSize(8);
        pool = new HikariDataSource(config); jdbc = new JdbcTemplate(pool);
        store = new FinalizedStore(jdbc, new DataSourceTransactionManager(pool), new FinalizedCodec(), meters);
        repository = new NotificationRepository(jdbc, new DataSourceTransactionManager(pool));
        server = new NotificationTestServer();
        settings = settings(262144, Duration.ofMillis(500));
        client = new CustomerNotificationClient(settings.httpTimeout());
    }

    @AfterEach void cleanup() {
        if (server != null) server.close();
        if (client != null) client.close();
        if (pool != null) { try { jdbc.execute("DROP SCHEMA " + schema + " CASCADE"); } finally { pool.close(); } }
        meters.close();
    }

    private NotificationProperties settings(int maxBytes, Duration timeout) {
        var destination = new NotificationProperties.Destination(server.url(), NotificationTestServer.TOKEN);
        return new NotificationProperties(true, 4, 10, timeout, Duration.ofSeconds(10), Duration.ofMillis(1), Duration.ofMillis(2), maxBytes, Map.of(42L, destination, 43L, destination));
    }
    private NotificationService service() { return new NotificationService(repository, client, settings, meters); }
    private DeliveryFinalized save(long tenant) {
        var e = FinalizedCodecTest.event("DELIVERED", 1);
        var result = new DeliveryFinalized(e.schemaVersion(), e.eventType(), e.eventId(), e.deliveryId(), tenant, e.deliveryType(), e.outcome(), e.reason(), e.routeOrder(), e.attemptId(), e.provider(), e.occurredAt(), e.resultAt(), e.finalizedAt(), e.deadline());
        store.save(result); return result;
    }
    private long count(String status) { return jdbc.queryForObject("SELECT count(*) FROM customer_notification_outbox WHERE status = ?", Long.class, status); }
    private void due() { jdbc.update("UPDATE customer_notification_batch SET next_attempt_at = clock_timestamp() - interval '1 second' WHERE status = 'PENDING'"); }
    private void expireLease() { jdbc.update("UPDATE customer_notification_batch SET lease_until = clock_timestamp() - interval '1 second' WHERE status = 'IN_FLIGHT'"); }

    @ParameterizedTest @ValueSource(ints = {1, 99, 100, 101})
    void sendsUpToOneHundredWithoutWaitingForFullBatch(int amount) {
        for (int i = 0; i < amount; i++) save(42);
        service().deliver(42); service().deliver(42);
        assertEquals(amount, count("DELIVERED"));
        assertEquals((amount + 99) / 100, server.requests.size());
        assertEquals(amount, server.effects.size());
        for (var request : server.requests) {
            assertTrue(request.batch().results().size() <= 100);
            assertEquals(42, request.batch().tenantId());
            assertEquals(request.batch().batchId(), request.key());
            assertEquals("Bearer " + NotificationTestServer.TOKEN, request.authorization());
            assertTrue(request.batch().results().stream().allMatch(r -> r.tenantId() == 42));
        }
    }

    @Test void byteLimitSplitsBatchesAndCustomersNeverMix() {
        settings = settings(16384, Duration.ofMillis(500));
        for (int i = 0; i < 101; i++) save(42);
        save(43);
        for (int i = 0; i < 15 && count("PENDING") > 0; i++) { service().deliver(42); service().deliver(43); }
        assertEquals(102, count("DELIVERED"));
        assertTrue(server.requests.size() > 3);
        for (var request : server.requests) {
            assertTrue(request.body().getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 16384);
            assertTrue(request.batch().results().stream().allMatch(r -> r.tenantId() == request.batch().tenantId()));
        }
    }

    @Test void twentyRetriesKeepSameBatchBodyAndBudgetAcrossReconstructedWorkers() {
        var event = save(42);
        server.status.set(503);
        for (int attempt = 1; attempt <= 21; attempt++) {
            repository = new NotificationRepository(jdbc, new DataSourceTransactionManager(pool));
            service().deliver(42);
            assertEquals(attempt, jdbc.queryForObject("SELECT attempt_count FROM customer_notification_outbox", Integer.class));
            due();
        }
        store.save(event);
        service().deliver(42);
        assertEquals(21, server.requests.size());
        assertEquals(1, server.requests.stream().map(NotificationTestServer.Received::body).distinct().count());
        assertEquals(1, server.requests.stream().map(NotificationTestServer.Received::key).distinct().count());
        assertEquals(1, count("EXHAUSTED"));
        assertEquals(0, count("DELIVERED"));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM delivery_history WHERE outcome = 'DELIVERED'", Integer.class));
    }

    @Test void retryDoesNotRunBeforeDueAndOriginalBatchDoesNotAbsorbNewResults() {
        save(42); server.status.set(503);
        service().deliver(42);
        jdbc.update("UPDATE customer_notification_batch SET next_attempt_at = clock_timestamp() + interval '1 hour'");
        service().deliver(42); assertEquals(1, server.requests.size());
        server.status.set(204); save(42); service().deliver(42);
        assertEquals(2, server.requests.size()); // New work can use the lane while an old retry is not due.
        assertEquals(1, count("DELIVERED"));
        due(); service().deliver(42);
        assertEquals(server.requests.get(0).body(), server.requests.get(2).body());
        assertNotEquals(server.requests.get(0).key(), server.requests.get(1).key());
        assertEquals(2, count("DELIVERED"));
    }

    @Test void responseLossRetriesSameIdsAndCustomerDeduplicationPreventsSecondEffect() {
        save(42); server.status.set(0);
        service().deliver(42);
        assertEquals(1, count("PENDING"));
        server.status.set(204); due(); service().deliver(42);
        assertEquals(1, count("DELIVERED"));
        assertEquals(1, server.effects.size());
        assertEquals(1, server.requests.stream().map(NotificationTestServer.Received::body).distinct().count());
        assertEquals(2, jdbc.queryForObject("SELECT attempt_count FROM customer_notification_outbox", Integer.class));
    }

    @Test void databaseFailureAfterHttpAckRetainsLeaseAndRestartRetriesWithoutLosingResult() {
        save(42);
        jdbc.execute("ALTER TABLE customer_notification_outbox ADD CONSTRAINT injected_failure CHECK (status = 'PENDING')");
        assertThrows(org.springframework.dao.DataAccessException.class, () -> service().deliver(42));
        assertEquals("IN_FLIGHT", jdbc.queryForObject("SELECT status FROM customer_notification_batch", String.class));
        assertEquals(1, count("PENDING"));
        jdbc.execute("ALTER TABLE customer_notification_outbox DROP CONSTRAINT injected_failure");
        expireLease(); repository = new NotificationRepository(jdbc, new DataSourceTransactionManager(pool));
        service().deliver(42);
        assertEquals(1, count("DELIVERED"));
        assertEquals(2, server.requests.size());
        assertEquals(1, server.effects.size());
        assertEquals(server.requests.get(0).body(), server.requests.get(1).body());
    }

    @Test void sqlOutageBackoffPreservesAcknowledgedBatchLeaseAndRetryBudget() throws Exception {
        save(42);
        settings = new NotificationProperties(true, 1, 10, settings.httpTimeout(), settings.lease(),
                settings.retryDelay(), settings.maxRetryDelay(), settings.maxBatchBytes(), Map.of(42L, settings.customers().get(42L)));
        repository = org.mockito.Mockito.spy(repository);
        org.mockito.Mockito.doThrow(new org.springframework.dao.DataAccessResourceFailureException("SQL unavailable after HTTP ack"))
                .doCallRealMethod().when(repository).complete(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyBoolean(),
                        org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
        var nanos = new java.util.concurrent.atomic.AtomicLong();
        var backoff = new event.common.recovery.FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30), nanos::get, () -> 1.0);
        try (var scheduler = new NotificationScheduler(service(), settings, meters, backoff)) {
            scheduler.tick(); await(() -> meters.get("delivery.notification.active").gauge().value() == 0);
            var lease = jdbc.queryForMap("SELECT batch_id,lease_token,lease_until,attempt_count FROM customer_notification_batch");
            assertEquals(1, server.requests.size()); assertEquals(1, count("PENDING"));
            assertEquals("IN_FLIGHT", jdbc.queryForObject("SELECT status FROM customer_notification_batch", String.class));
            for (int i = 0; i < 100; i++) scheduler.tick();
            assertEquals(lease, jdbc.queryForMap("SELECT batch_id,lease_token,lease_until,attempt_count FROM customer_notification_batch"));
            assertEquals(1, server.requests.size());
            nanos.addAndGet(Duration.ofSeconds(1).toNanos());
            scheduler.tick(); await(() -> meters.get("delivery.notification.active").gauge().value() == 0);
            assertEquals(lease, jdbc.queryForMap("SELECT batch_id,lease_token,lease_until,attempt_count FROM customer_notification_batch"));
            expireLease(); scheduler.tick(); await(() -> meters.get("delivery.notification.active").gauge().value() == 0);
            assertEquals(1, count("DELIVERED")); assertEquals(2, server.requests.size());
            assertEquals(1, server.effects.size());
            assertEquals(server.requests.get(0).body(), server.requests.get(1).body());
            assertEquals(server.requests.get(0).key(), server.requests.get(1).key());
            assertEquals(2, jdbc.queryForObject("SELECT attempt_count FROM customer_notification_outbox", Integer.class));
            assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM delivery_history", Integer.class));
        }
    }

    @Test void claimRollbackDoesNotSpendAttemptOrSendHttp() {
        save(42);
        jdbc.execute("ALTER TABLE customer_notification_outbox ADD CONSTRAINT injected_failure CHECK (attempt_count = 0)");
        assertThrows(org.springframework.dao.DataAccessException.class, () -> service().deliver(42));
        assertEquals(0, server.requests.size());
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM customer_notification_batch", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT attempt_count FROM customer_notification_outbox", Integer.class));
        assertNull(jdbc.queryForObject("SELECT batch_id FROM customer_notification_outbox", UUID.class));
        jdbc.execute("ALTER TABLE customer_notification_outbox DROP CONSTRAINT injected_failure");
        service().deliver(42); assertEquals(1, count("DELIVERED"));
    }

    @Test void parallelClaimHasOneOwnerAndExpiredOwnerCannotFinishNewAttempt() throws Exception {
        save(42);
        List<NotificationRepository.Claim> claims = new ArrayList<>();
        try (var threads = Executors.newFixedThreadPool(8)) {
            List<Future<Optional<NotificationRepository.Claim>>> work = new ArrayList<>();
            for (int i = 0; i < 16; i++) work.add(threads.submit(() -> repository.claim(42, settings.customers().get(42L), settings)));
            for (var result : work) result.get(15, TimeUnit.SECONDS).ifPresent(claims::add);
        }
        assertEquals(1, claims.size());
        var old = claims.getFirst(); assertTrue(repository.owns(old));
        expireLease();
        var current = repository.claim(42, settings.customers().get(42L), settings).orElseThrow();
        assertEquals(old.batchId(), current.batchId()); assertEquals(old.body(), current.body()); assertEquals(2, current.attempt());
        assertFalse(repository.owns(old));
        assertFalse(repository.complete(old, true, null, Duration.ZERO));
        assertTrue(repository.complete(current, true, null, Duration.ZERO));
        assertEquals(1, count("DELIVERED"));
    }

    @Test void twentyOneUnconfirmedReservationsExhaustWithoutTwentySecondCall() {
        save(42);
        NotificationRepository.Claim first = null;
        for (int attempt = 1; attempt <= 21; attempt++) {
            var claim = repository.claim(42, settings.customers().get(42L), settings).orElseThrow();
            if (first == null) first = claim;
            assertEquals(attempt, claim.attempt()); expireLease();
        }
        assertTrue(repository.claim(42, settings.customers().get(42L), settings).isEmpty());
        assertEquals(1, count("EXHAUSTED"));
        assertEquals("LAST_ATTEMPT_UNCONFIRMED", jdbc.queryForObject("SELECT last_error FROM customer_notification_batch", String.class));
        assertFalse(repository.complete(first, true, null, Duration.ZERO));
        assertEquals(0, server.requests.size());
    }

    @Test void changedDestinationRetainsOldBatchWithoutSpendingBudget() {
        save(42); server.status.set(503); service().deliver(42); due();
        var changed = new NotificationProperties.Destination(server.url().resolve("/other"), NotificationTestServer.TOKEN);
        assertTrue(repository.claim(42, changed, settings).isEmpty());
        assertEquals(1, jdbc.queryForObject("SELECT attempt_count FROM customer_notification_batch", Integer.class));
        assertEquals(1, count("PENDING"));
        server.status.set(204); service().deliver(42); assertEquals(1, count("DELIVERED"));
    }

    @ParameterizedTest @ValueSource(ints = {200, 207, 302, 401, 429, 500})
    void only204AcknowledgesWholeBatchAndRedirectIsNeverFollowed(int code) {
        save(42); server.status.set(code); service().deliver(42);
        assertEquals(1, count("PENDING")); assertEquals(0, count("DELIVERED"));
        assertEquals(0, server.redirects.get()); assertEquals(1, server.requests.size());
        assertEquals("HTTP_" + code, jdbc.queryForObject("SELECT last_error FROM customer_notification_batch", String.class));
    }

    @Test void slowCustomerDoesNotBlockOtherCustomerAndOneResultIsFlushedOnPoll() throws Exception {
        settings = settings(262144, Duration.ofSeconds(3));
        client.close(); client = new CustomerNotificationClient(settings.httpTimeout());
        save(42); save(43); server.slowTenant = 42;
        try (var scheduler = new NotificationScheduler(service(), settings, meters)) {
            scheduler.tick();
            assertTrue(server.slowStarted.await(5, TimeUnit.SECONDS));
            await(() -> count("DELIVERED") == 1);
            assertEquals(43, jdbc.queryForObject("SELECT tenant_id FROM customer_notification_batch WHERE status = 'DELIVERED'", Long.class));
            assertEquals("IN_FLIGHT", jdbc.queryForObject("SELECT status FROM customer_notification_batch WHERE tenant_id = 42", String.class));
            server.releaseSlow.countDown();
            await(() -> count("DELIVERED") == 2);
        }
    }

    @Test void timeoutReturnsToPendingWithoutHoldingDatabaseLock() throws Exception {
        save(42); server.slowTenant = 42;
        try (var worker = Executors.newSingleThreadExecutor()) {
            var call = worker.submit(() -> service().deliver(42));
            assertTrue(server.slowStarted.await(5, TimeUnit.SECONDS));
            assertTrue(repository.claim(42, settings.customers().get(42L), settings).isEmpty());
            call.get(3, TimeUnit.SECONDS);
            assertEquals(1, count("PENDING"));
            assertEquals(1, jdbc.queryForObject("SELECT attempt_count FROM customer_notification_outbox", Integer.class));
            server.releaseSlow.countDown();
        }
    }

    @Test void unconfiguredCustomerKeepsPendingWithoutSpendingAttempts() {
        save(44); service().deliver(44);
        assertEquals(1, count("PENDING"));
        assertEquals(0, jdbc.queryForObject("SELECT attempt_count FROM customer_notification_outbox", Integer.class));
        assertEquals(0, server.requests.size());
    }

    @Test void versionOnePendingRowsSurviveMigrationAndCanBeNotified() {
        jdbc.execute("DROP SCHEMA " + schema + " CASCADE");
        String url = System.getenv("POSTGRES_TEST_URL");
        String user = System.getenv().getOrDefault("POSTGRES_TEST_USER", "delivery"), password = System.getenv().getOrDefault("POSTGRES_TEST_PASSWORD", "delivery");
        Flyway.configure().dataSource(url, user, password).defaultSchema(schema).schemas(schema).target("1").load().migrate();
        var event = FinalizedCodecTest.event("DELIVERED", 1);
        jdbc.update("INSERT INTO delivery_history(delivery_id, result_event_id, tenant_id, delivery_type, outcome, reason, route_order, attempt_id, provider, occurred_at, result_at, finalized_at, deadline, result_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb))",
                UUID.fromString(event.deliveryId()), UUID.fromString(event.eventId()), event.tenantId(), event.deliveryType(), event.outcome(), event.reason(), event.routeOrder(), UUID.fromString(event.attemptId()), event.provider(),
                event.occurredAt().atOffset(java.time.ZoneOffset.UTC), event.resultAt().atOffset(java.time.ZoneOffset.UTC), event.finalizedAt().atOffset(java.time.ZoneOffset.UTC), event.deadline().atOffset(java.time.ZoneOffset.UTC), new FinalizedCodec().encode(event));
        jdbc.update("INSERT INTO customer_notification_outbox(result_event_id) VALUES (?)", UUID.fromString(event.eventId()));
        Flyway.configure().dataSource(url, user, password).defaultSchema(schema).schemas(schema).load().migrate();
        service().deliver(42);
        assertEquals(1, count("DELIVERED"));
        assertEquals(event.eventId(), server.requests.getFirst().batch().results().getFirst().eventId());
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long end = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < end) Thread.sleep(10);
        assertTrue(condition.getAsBoolean());
    }
}

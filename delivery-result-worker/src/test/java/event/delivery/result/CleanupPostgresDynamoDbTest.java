package event.delivery.result;

import com.zaxxer.hikari.*;
import event.common.delivery.*;
import event.common.lifecycle.*;
import event.delivery.result.cleanup.*;
import event.delivery.result.notification.*;
import event.delivery.result.operations.DeliveryOperations;
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
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.*;
import static event.common.dynamodb.DynamoDbTableNames.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@EnabledIfEnvironmentVariable(named = "POSTGRES_TEST_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "DYNAMODB_TEST_ENDPOINT", matches = ".+")
class CleanupPostgresDynamoDbTest {
    final String schema = "cleanup_test_" + UUID.randomUUID().toString().replace("-", "");
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    final List<DeliveryFinalized> results = new ArrayList<>();
    final List<CleanupWorker> workers = new ArrayList<>();
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
        new event.common.dynamodb.config.DynamoDbTableInitializer(db).run(null);
        store = new FinalizedStore(jdbc, new DataSourceTransactionManager(pool), new FinalizedCodec(), meters);
        repository = new CleanupRepository(jdbc, new DataSourceTransactionManager(pool));
    }
    @AfterEach void cleanup() {
        workers.forEach(CleanupWorker::close);
        if (db != null) {
            for (var e : results) for (String sk : List.of("META", "FINAL", "ATTEMPT#" + e.attemptId()))
                db.deleteItem(r -> r.tableName(tableForKey(key(e, sk))).key(key(e, sk)));
            for (var e : results) db.deleteItem(r -> r.tableName(tableForKey(DeliveryCompletion.metaKey(e.requestKey()))).key(DeliveryCompletion.metaKey(e.requestKey())));
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
        db.putItem(r -> r.tableName(tableForKey(meta)).item(meta));
        var attempt = new HashMap<>(key(e, "ATTEMPT#" + e.attemptId()));
        attempt.put("attempt_id", s(e.attemptId())); attempt.put("version", AttributeValue.fromN("2"));
        attempt.put("lifecycle_closed", AttributeValue.fromBool(true)); attempt.put(DeliveryCompletion.TRACKING_VERSION, AttributeValue.fromN("1"));
        db.putItem(r -> r.tableName(tableForKey(attempt)).item(attempt));
        var completed = new HashMap<>(key(e, "FINAL")); completed.put("result_event", s(new FinalizedCodec().encode(e))); completed.put("publish_state", s("PUBLISHED"));
        db.putItem(r -> r.tableName(tableForKey(completed)).item(completed)); return e;
    }
    Map<String, AttributeValue> meta(DeliveryFinalized e) { return db.getItem(r -> r.tableName(tableForKey(key(e, "META"))).key(key(e, "META")).consistentRead(true)).item(); }
    static Map<String, AttributeValue> key(DeliveryFinalized e, String sk) { return Map.of("pk", s("DELIVERY#" + e.deliveryId()), "sk", s(sk)); }
    static AttributeValue s(String value) { return AttributeValue.fromS(value); }
    CleanupWorker worker(CleanupRepository repository) {
        var worker = new CleanupWorker(repository, new DeliveryCompactor(db), meters);
        workers.add(worker); return worker;
    }

    DeliveryFinalized seedModern(String requestKey) {
        var old = seed();
        var e = new DeliveryFinalized(2, old.eventType(), old.eventId(), old.deliveryId(), old.tenantId(), old.deliveryType(),
                old.outcome(), old.reason(), old.routeOrder(), old.attemptId(), old.provider(), old.occurredAt(), old.resultAt(),
                java.time.Instant.now(), old.deadline(), requestKey);
        results.add(e);
        var origin = new HashMap<>(meta(old)); origin.putAll(DeliveryCompletion.metaKey(requestKey));
        origin.put("schema_version", AttributeValue.fromN("2")); origin.put("request_key", s(requestKey));
        db.putItem(r -> r.tableName(tableForKey(origin)).item(origin));
        db.deleteItem(r -> r.tableName(tableForKey(key(old, "META"))).key(key(old, "META")));
        db.deleteItem(r -> r.tableName(tableForKey(key(old, "FINAL"))).key(key(old, "FINAL")));
        db.updateItem(r -> r.tableName(tableForKey(key(old, "ATTEMPT#" + old.attemptId()))).key(key(old, "ATTEMPT#" + old.attemptId()))
                .updateExpression("SET result_event = :result, publish_state = :published")
                .expressionAttributeValues(Map.of(":result", s(new FinalizedCodec().encode(e)), ":published", s("PUBLISHED"))));
        return e;
    }
    Map<String, AttributeValue> origin(DeliveryFinalized e) {
        return db.getItem(r -> r.tableName(tableForKey(DeliveryCompletion.metaKey(e.requestKey()))).key(DeliveryCompletion.metaKey(e.requestKey())).consistentRead(true)).item();
    }
    DeliveryOperations operations(Instant now) { return new DeliveryOperations(jdbc, db, Clock.fixed(now, ZoneOffset.UTC)); }
    void activeStep(DeliveryFinalized e, String state, Instant deadline, Instant lease) {
        db.updateItem(r -> r.tableName(ORIGIN).key(DeliveryCompletion.metaKey(e.requestKey()))
                .updateExpression("REMOVE completion_event_id"));
        db.updateItem(r -> r.tableName(STEP).key(key(e, "ATTEMPT#" + e.attemptId()))
                .updateExpression("SET #status=:state, deadline_at=:deadline, lease_until=:lease, review_reason=:reason, provider=:provider, route_order=:route "
                        + "REMOVE lifecycle_closed,result_event,publish_state")
                .expressionAttributeNames(Map.of("#status", "status"))
                .expressionAttributeValues(Map.of(":state", s(state), ":deadline", AttributeValue.fromN(Long.toString(deadline.toEpochMilli())),
                        ":lease", AttributeValue.fromN(Long.toString(lease.toEpochMilli())), ":reason", s("LEASE_EXPIRED_WITHOUT_RESULT"),
                        ":provider", s("test-provider"), ":route", AttributeValue.fromN("1"))));
    }

    @Test void exhaustedOperationsAreTenantScopedPagedAndDoNotResetAttempts() {
        jdbc.update("INSERT INTO customer_notification_lane(tenant_id) VALUES (42),(43)");
        for (int i = 0; i < 4; i++) jdbc.update("INSERT INTO customer_notification_batch(batch_id,tenant_id,destination_url,request_body,item_count,status,attempt_count,last_error) "
                        + "VALUES (?,?,?, ?,1,'EXHAUSTED',21,'HTTP_503')", UUID.randomUUID(), i == 3 ? 43L : 42L, "http://private-customer", "private-request-body");
        var before = jdbc.queryForList("SELECT * FROM customer_notification_batch ORDER BY batch_id");
        var operations = new DeliveryOperations(jdbc, null, Clock.systemUTC());
        var first = operations.exhausted(42, null, 2); assertEquals(2, first.records().size()); assertTrue(first.hasMore());
        var second = operations.exhausted(42, first.nextAfterId(), 2); assertEquals(1, second.records().size()); assertFalse(second.hasMore());
        assertNotEquals(first.records().getFirst().batchId(), second.records().getFirst().batchId());
        assertEquals(21, second.records().getFirst().attempts()); assertEquals("HTTP_503", second.records().getFirst().lastError());
        String json = JsonMapper.builder().build().writeValueAsString(first);
        assertFalse(json.contains("private-request-body")); assertFalse(json.contains("private-customer"));
        assertEquals(before, jdbc.queryForList("SELECT * FROM customer_notification_batch ORDER BY batch_id"));
        assertThrows(IllegalArgumentException.class, () -> operations.exhausted(42, null, 101));
    }

    @Test void cleanupOperationsDistinguishDueBackoffLeaseAndMissingNotificationWithoutClaiming() {
        var due = seedModern(UUID.randomUUID().toString()); store.save(due);
        var backoff = seedModern(UUID.randomUUID().toString()); store.save(backoff);
        var leased = seedModern(UUID.randomUUID().toString()); store.save(leased);
        var expired = seedModern(UUID.randomUUID().toString()); store.save(expired);
        var missing = seedModern(UUID.randomUUID().toString()); store.save(missing);
        jdbc.update("UPDATE delivery_cleanup_outbox SET next_attempt_at=clock_timestamp()+interval '10 minutes' WHERE result_event_id=?", UUID.fromString(backoff.eventId()));
        jdbc.update("UPDATE delivery_cleanup_outbox SET lease_token=?,lease_until=clock_timestamp()+interval '10 minutes' WHERE result_event_id=?", UUID.randomUUID(), UUID.fromString(leased.eventId()));
        jdbc.update("UPDATE delivery_cleanup_outbox SET lease_token=?,lease_until=clock_timestamp()-interval '10 minutes' WHERE result_event_id=?", UUID.randomUUID(), UUID.fromString(expired.eventId()));
        jdbc.update("DELETE FROM customer_notification_outbox WHERE result_event_id=?", UUID.fromString(missing.eventId()));
        var before = jdbc.queryForList("SELECT * FROM delivery_cleanup_outbox ORDER BY result_event_id");
        var operations = operations(Instant.now().plusSeconds(1));
        var first = operations.cleanup(42, null, 2); var second = operations.cleanup(42, first.nextAfterId(), 100);
        var rows = new ArrayList<>(first.records()); rows.addAll(second.records()); assertEquals(5, rows.size());
        var states = new HashMap<UUID, String>(); rows.forEach(row -> states.put(row.resultEventId(), row.attention()));
        assertEquals("DUE", states.get(UUID.fromString(due.eventId()))); assertEquals("BACKOFF", states.get(UUID.fromString(backoff.eventId())));
        assertEquals("LEASED", states.get(UUID.fromString(leased.eventId()))); assertEquals("LEASE_EXPIRED", states.get(UUID.fromString(expired.eventId())));
        assertEquals("MISSING_NOTIFICATION", states.get(UUID.fromString(missing.eventId())));
        assertTrue(operations.cleanup(43, null, 100).records().isEmpty());
        assertEquals(before, jdbc.queryForList("SELECT * FROM delivery_cleanup_outbox ORDER BY result_event_id"));
    }

    @Test void requestOperationsObserveUnconfirmedLeaseWithoutWritingOrExposingPayload() {
        var e = seedModern(UUID.randomUUID().toString()); Instant now = Instant.now();
        activeStep(e, "PROCESSING", now.plusSeconds(3600), now.minusSeconds(10));
        var beforeOrigin = origin(e);
        var beforeStep = db.getItem(r -> r.tableName(STEP).key(key(e, "ATTEMPT#" + e.attemptId())).consistentRead(true)).item();
        var view = operations(now).request(42, e.requestKey());
        assertEquals("ACTIVE", view.active().observation()); assertEquals(e.deliveryId(), view.active().executionId());
        assertEquals("RESULT_UNCONFIRMED", view.active().attempts().getFirst().attention());
        assertEquals("PROCESSING", view.active().attempts().getFirst().state()); assertTrue(view.recentHistory().isEmpty());
        assertFalse(JsonMapper.builder().build().writeValueAsString(view).contains("payload"));
        assertEquals(beforeOrigin, origin(e));
        assertEquals(beforeStep, db.getItem(r -> r.tableName(STEP).key(key(e, "ATTEMPT#" + e.attemptId())).consistentRead(true)).item());
    }

    @Test void requestOperationsKeepReviewAndExpiryAsObservationsInsteadOfFinalResults() {
        var e = seedModern(UUID.randomUUID().toString()); Instant now = Instant.now();
        activeStep(e, "REVIEW_REQUIRED", now.plusSeconds(3600), now.minusSeconds(10));
        var operations = operations(now);
        assertEquals("REVIEW_REQUIRED", operations.request(42, e.requestKey()).active().attempts().getFirst().attention());
        activeStep(e, "REVIEW_REQUIRED", now.minusSeconds(1), now.minusSeconds(10));
        var expired = operations.request(42, e.requestKey());
        assertEquals("EXPIRY_DUE", expired.active().attempts().getFirst().attention());
        assertEquals("REVIEW_REQUIRED", expired.active().attempts().getFirst().state()); assertTrue(expired.recentHistory().isEmpty());
    }

    @Test void requestOperationsFindCommittedHistoryAfterNormalDynamoCleanup() {
        var e = seedModern(UUID.randomUUID().toString()); store.save(e); worker(repository).tick();
        var view = operations(Instant.now()).request(42, e.requestKey());
        assertEquals("NO_ACTIVE_ORIGIN", view.active().observation()); assertEquals(1, view.recentHistory().size());
        assertEquals("DELIVERED", view.recentHistory().getFirst().outcome()); assertEquals("DONE", view.recentHistory().getFirst().cleanupState());
        assertEquals("PENDING", view.recentHistory().getFirst().notificationState());
        assertTrue(operations(Instant.now()).cleanup(42, null, 10).records().isEmpty());
        var absent = operations(Instant.now()).request(42, UUID.randomUUID().toString());
        assertEquals("NO_ACTIVE_ORIGIN", absent.active().observation()); assertTrue(absent.recentHistory().isEmpty());
    }

    @Test void requestOperationsSeparateGenerationsAndHideOtherTenants() {
        String requestKey = UUID.randomUUID().toString(); var first = seedModern(requestKey); store.save(first);
        var current = seedModern(requestKey); store.save(current);
        var view = operations(Instant.now()).request(42, requestKey);
        assertEquals(current.deliveryId(), view.active().executionId()); assertEquals(2, view.recentHistory().size());
        assertEquals(current.attemptId(), view.active().attempts().getFirst().attemptId());
        var other = operations(Instant.now()).request(43, requestKey);
        assertEquals("NO_ACTIVE_ORIGIN", other.active().observation()); assertNull(other.active().executionId()); assertTrue(other.recentHistory().isEmpty());
    }

    @Test void requestOperationsDiscardAttemptSnapshotWhenOriginChangesDuringRead() {
        var e = seedModern(UUID.randomUUID().toString());
        var observing = mock(DynamoDbClient.class); var calls = new java.util.concurrent.atomic.AtomicInteger();
        doAnswer(invocation -> {
            if (calls.incrementAndGet() == 2) db.updateItem(r -> r.tableName(ORIGIN).key(DeliveryCompletion.metaKey(e.requestKey()))
                    .updateExpression("SET delivery_id=:next").expressionAttributeValues(Map.of(":next", s(UUID.randomUUID().toString()))));
            return db.getItem((java.util.function.Consumer<software.amazon.awssdk.services.dynamodb.model.GetItemRequest.Builder>) invocation.getArgument(0));
        }).when(observing).getItem(any(java.util.function.Consumer.class));
        doAnswer(invocation -> db.query((java.util.function.Consumer<software.amazon.awssdk.services.dynamodb.model.QueryRequest.Builder>) invocation.getArgument(0)))
                .when(observing).query(any(java.util.function.Consumer.class));
        var view = new DeliveryOperations(jdbc, observing, Clock.systemUTC()).request(42, e.requestKey());
        assertEquals("CHANGED_DURING_READ", view.active().observation()); assertNull(view.active().executionId()); assertTrue(view.active().attempts().isEmpty());
    }

    @Test void failedStorageLookupNeverBecomesAnEmptySuccessfulObservation() {
        var e = seedModern(UUID.randomUUID().toString()); store.save(e);
        var unavailable = mock(DynamoDbClient.class);
        doThrow(new IllegalStateException("DynamoDB unavailable")).when(unavailable).getItem(any(java.util.function.Consumer.class));
        assertThrows(IllegalStateException.class, () -> new DeliveryOperations(jdbc, unavailable, Clock.systemUTC()).request(42, e.requestKey()));
        var failedSql = new JdbcTemplate(pool) {
            @Override public <T> List<T> query(String query, org.springframework.jdbc.core.RowMapper<T> mapper, Object... args) {
                throw new org.springframework.dao.DataAccessResourceFailureException("SQL unavailable");
            }
        };
        var untouched = mock(DynamoDbClient.class);
        assertThrows(org.springframework.dao.DataAccessResourceFailureException.class, () -> new DeliveryOperations(failedSql, untouched, Clock.systemUTC()).request(42, e.requestKey()));
        verifyNoInteractions(untouched);
    }

    @Test void modernCleanupRequiresSqlThenDeletesOriginAndStepWithoutPermanentMarker() {
        var e = seedModern(UUID.randomUUID().toString());
        worker(repository).tick(); assertTrue(origin(e).containsKey("payload"));
        store.save(e); worker(repository).tick(); assertTrue(origin(e).isEmpty());
        assertTrue(db.query(r -> r.tableName(STEP).keyConditionExpression("pk = :pk")
                .expressionAttributeValues(Map.of(":pk", s("DELIVERY#" + e.deliveryId())))).items().isEmpty());
        assertEquals("PENDING", jdbc.queryForObject("SELECT status FROM customer_notification_outbox", String.class));
        assertEquals("DONE", jdbc.queryForObject("SELECT status FROM delivery_cleanup_outbox", String.class));
    }
    @Test void sqlDoneLossThenNewGenerationPreservesNewOriginAndSeparateHistoryAndNotifications() {
        String requestKey = UUID.randomUUID().toString(); var first = seedModern(requestKey); store.save(first);
        var failing = spy(repository); doThrow(new IllegalStateException("SQL done response unavailable")).when(failing).done(any());
        worker(failing).tick(); assertTrue(origin(first).isEmpty());
        var second = seedModern(requestKey); var before = origin(second);
        jdbc.update("UPDATE delivery_cleanup_outbox SET next_attempt_at = clock_timestamp() - interval '1 second'");
        worker(repository).tick(); assertEquals(before, origin(second));
        store.save(second); worker(repository).tick();
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM delivery_history", Integer.class));
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM customer_notification_outbox", Integer.class));
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM delivery_cleanup_outbox WHERE status = 'DONE'", Integer.class));
        assertEquals(FinalizedStore.Outcome.DUPLICATE, store.save(first));
    }

    @Test void legacyCompletionHashWithoutRequestKeyRemainsReadable() {
        var e = seed(); store.save(e); worker(repository).tick();
        var mapper = JsonMapper.builder().build();
        var oldJson = mapper.readValue(new FinalizedCodec().encode(e), new tools.jackson.core.type.TypeReference<LinkedHashMap<String, Object>>() {});
        oldJson.remove("requestKey");
        db.updateItem(r -> r.tableName(tableForKey(key(e, "META"))).key(key(e, "META")).updateExpression("SET result_hash = :hash")
                .expressionAttributeValues(Map.of(":hash", s(DeliveryCompletion.hash("result:v1", mapper.writeValueAsString(oldJson))))));
        assertFalse(new DeliveryCompactor(db).compact(e));
    }

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

    @Test void staleOwnerCannotCompleteOrPostponeNewOwnersClaim() {
        var e = seedModern(UUID.randomUUID().toString()); store.save(e);
        var stale = repository.claim().orElseThrow();
        jdbc.update("UPDATE delivery_cleanup_outbox SET lease_until = clock_timestamp() - interval '1 second'");
        var current = repository.claim().orElseThrow();
        assertNotEquals(stale.token(), current.token());
        assertFalse(repository.done(stale));
        repository.retry(stale);
        assertEquals(current.token(), jdbc.queryForObject("SELECT lease_token FROM delivery_cleanup_outbox", UUID.class));
        assertEquals("PENDING", jdbc.queryForObject("SELECT status FROM delivery_cleanup_outbox", String.class));
        assertTrue(origin(e).containsKey("payload"));
        assertTrue(new DeliveryCompactor(db).compact(current.result()));
        assertTrue(repository.done(current));
        assertFalse(repository.done(stale));
    }

    @Test void concurrentWorkersClaimDistinctResultsAndDeleteOnlyAfterSqlCommit() throws Exception {
        var saved = new ArrayList<DeliveryFinalized>();
        for (int i = 0; i < 12; i++) { var e = seedModern(UUID.randomUUID().toString()); store.save(e); saved.add(e); }
        var unsaved = seedModern(UUID.randomUUID().toString());
        var calls = new ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicInteger>();
        var firstClaims = new CountDownLatch(4);
        var release = new CountDownLatch(1);
        var compactor = spy(new DeliveryCompactor(db));
        doAnswer(invocation -> {
            DeliveryFinalized e = invocation.getArgument(0);
            calls.computeIfAbsent(e.eventId(), key -> new java.util.concurrent.atomic.AtomicInteger()).incrementAndGet();
            firstClaims.countDown();
            assertTrue(release.await(10, TimeUnit.SECONDS));
            return invocation.callRealMethod();
        }).when(compactor).compact(any());
        try (var one = new CleanupWorker(repository, compactor, meters, 2, 20);
             var two = new CleanupWorker(new CleanupRepository(jdbc, new DataSourceTransactionManager(pool)), compactor, meters, 2, 20);
             var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(one::tick); var b = executor.submit(two::tick);
            try {
                assertTrue(firstClaims.await(10, TimeUnit.SECONDS));
                assertEquals(4, calls.size());
                assertEquals(4, jdbc.queryForObject("SELECT count(*) FROM delivery_cleanup_outbox WHERE lease_token IS NOT NULL", Integer.class));
            } finally { release.countDown(); }
            a.get(20, TimeUnit.SECONDS); b.get(20, TimeUnit.SECONDS);
        }
        assertEquals(12, calls.size()); assertTrue(calls.values().stream().allMatch(c -> c.get() == 1));
        assertEquals(12, jdbc.queryForObject("SELECT count(*) FROM delivery_cleanup_outbox WHERE status = 'DONE'", Integer.class));
        for (var e : saved) {
            assertTrue(origin(e).isEmpty());
            assertTrue(db.query(r -> r.tableName(STEP).keyConditionExpression("pk = :pk")
                    .expressionAttributeValues(Map.of(":pk", s("DELIVERY#" + e.deliveryId())))).items().isEmpty());
        }
        assertTrue(origin(unsaved).containsKey("payload"));
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

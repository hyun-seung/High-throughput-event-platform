package event.delivery.ingress.repository;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryIds;
import event.common.dynamodb.config.DynamoDbTableInitializer;
import event.delivery.ingress.consumer.DeliveryRequestConsumer;
import event.delivery.ingress.service.DeliveryFlowProducer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

import static event.common.dynamodb.DynamoDbAttributeNames.*;
import static event.common.dynamodb.DynamoDbTableNames.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** Opt-in test against DynamoDB Local. Removes only its own UUID-keyed records. */
@EnabledIfEnvironmentVariable(named = "DYNAMODB_TEST_ENDPOINT", matches = ".+")
class DeliveryIngressDynamoDbTest {

    private static DynamoDbClient client;
    private DeliveryRepository repository;
    private DeliveryFlowProducer producer;
    private DeliveryRequestConsumer consumer;
    private DeliveryEvent first;

    @BeforeAll
    static void connect() {
        URI endpoint = URI.create(System.getenv("DYNAMODB_TEST_ENDPOINT"));
        if (!"http".equals(endpoint.getScheme())
                || !("localhost".equals(endpoint.getHost()) || "127.0.0.1".equals(endpoint.getHost()))) {
            throw new IllegalArgumentException("DYNAMODB_TEST_ENDPOINT must point to local HTTP DynamoDB");
        }
        client = DynamoDbClient.builder()
                .endpointOverride(endpoint)
                .region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .overrideConfiguration(config -> config.apiCallTimeout(Duration.ofSeconds(10)))
                .build();
        new DynamoDbTableInitializer(client).run(null);
    }

    @BeforeEach
    void setUp() {
        repository = new DeliveryRepository(client, JsonMapper.builder().build());
        producer = mock(DeliveryFlowProducer.class);
        consumer = new DeliveryRequestConsumer(repository, producer, metrics());
        first = DeliveryEvent.requested(DeliveryIds.deliveryId(999L, "ingress-test-" + UUID.randomUUID()),
                999L, "EMAIL", Map.of("body", "hello", "recipient", "test"),
                Instant.parse("2026-09-23T00:00:00Z"));
    }

    @AfterEach
    void removeOwnRecord() {
        client.deleteItem(request -> request.tableName(tableForKey(key())).key(key()));
    }

    @AfterAll
    static void close() {
        if (client != null) client.close();
    }

    @Test void newAdmissionKeepsOneExecutionAcrossConcurrentApiRetriesAndRedisLoss() throws Exception {
        first = first.forAdmission();
        var executions = new java.util.HashSet<String>();
        try (var workers = Executors.newFixedThreadPool(8)) {
            var calls = new ArrayList<Future<DeliveryRepository.SavedDelivery>>();
            for (int i = 0; i < 24; i++) calls.add(workers.submit(() -> repository.saveOrLoad(first)));
            for (var call : calls) {
                var saved = call.get(10, TimeUnit.SECONDS);
                assertFalse(saved.completed()); assertEquals(first.occurredAt(), saved.event().occurredAt());
                assertEquals(first.requestKey(), saved.event().requestKey()); executions.add(saved.event().deliveryId());
            }
        }
        assertEquals(1, executions.size()); assertFalse(executions.contains(first.deliveryId()));
        assertEquals(executions.iterator().next(), new DeliveryRepository(client, JsonMapper.builder().build()).saveOrLoad(first).event().deliveryId());
    }

    @Test void realRedisCompletionBlocksUntilLostAndActiveDynamoStillWins() throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("REDIS_TEST_PORT", "16379"));
        var factory = new org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory("localhost", port);
        factory.afterPropertiesSet(); factory.start();
        var redis = new org.springframework.data.redis.core.StringRedisTemplate(factory);
        var cache = new event.common.redis.DeliveryCache(redis, null, Duration.ofSeconds(30));
        first = first.forAdmission();
        try {
            var repo = new DeliveryRepository(client, JsonMapper.builder().build(), cache);
            var active = repo.saveOrLoad(first).event();
            cache.removeSchedule(active.requestKey());
            assertEquals(active.deliveryId(), repo.saveOrLoad(first).event().deliveryId());
            var fingerprint = event.common.lifecycle.DeliveryCompletion.fingerprint(first.requestKey(), first.tenantId(), first.deliveryType(),
                    first.fallbackAllowed(), JsonMapper.builder().build().writeValueAsString(first.payload()));
            cache.complete(first.requestKey(), fingerprint, Instant.now());
            assertTrue(repo.saveOrLoad(first).completed());
            // Simulate the deletion boundary; SQL-before-cleanup is tested in the result worker.
            client.deleteItem(r -> r.tableName(tableForKey(key())).key(key()));
            assertTrue(repo.saveOrLoad(first).completed());
            redis.delete("delivery:completed:" + first.requestKey());
            var next = repo.saveOrLoad(first).event(); assertNotEquals(active.deliveryId(), next.deliveryId());
            assertEquals(first.requestKey(), next.requestKey());
            cache.removeSchedule(next.deliveryId()); cache.removeSchedule(active.deliveryId());
        } finally { cache.removeSchedule(first.requestKey()); redis.delete("delivery:completed:" + first.requestKey()); factory.destroy(); }
    }

    @Test void cacheCompletionArrivingAfterAdmissionNeverDeletesAnActiveOrigin() {
        first = first.forAdmission();
        var cache = mock(event.common.redis.DeliveryCache.class);
        when(cache.completed(first.requestKey())).thenReturn(null, "completion-arrived");
        var repo = new DeliveryRepository(client, JsonMapper.builder().build(), cache);
        var saved = repo.saveOrLoad(first); assertFalse(saved.completed());
        assertEquals(saved.event().deliveryId(), stored().get("delivery_id").s());
    }

    @Test
    void replayAfterStorageBeforePublicationRecoversDispatchWithOriginalTime() {
        repository.saveOrLoad(first); // Process stopped after this durable write.
        var restarted = new DeliveryRequestConsumer(new DeliveryRepository(client, JsonMapper.builder().build()), producer, metrics());
        when(producer.sendDispatchRequested(any())).thenReturn(CompletableFuture.completedFuture(null));

        restarted.consume(retryAt(first.occurredAt().plusSeconds(3600)));

        verify(producer).sendDispatchRequested(first.toDispatchRequested());
        assertEquals(first.occurredAt().toString(), stored().get(OCCURRED_AT).s());
    }

    @Test
    void replayAfterPublicationWithLostAckUsesSameDispatchIdAndIngressTime() {
        when(producer.sendDispatchRequested(any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("lost dispatch ack")))
                .thenReturn(CompletableFuture.completedFuture(null));

        assertThrows(CompletionException.class, () -> consumer.consume(first));
        consumer.consume(retryAt(first.occurredAt().plusSeconds(3600)));

        var events = ArgumentCaptor.forClass(DeliveryEvent.class);
        verify(producer, times(2)).sendDispatchRequested(events.capture());
        assertEquals(first.toDispatchRequested(), events.getAllValues().get(0));
        assertEquals(events.getAllValues().get(0), events.getAllValues().get(1));
    }

    @Test
    void differentPayloadWithSameKeyDoesNotOverwriteOrDispatch() {
        repository.saveOrLoad(first);
        var collision = DeliveryEvent.requested(first.deliveryId(), first.tenantId(), first.deliveryType(),
                Map.of("body", "different"), first.occurredAt().plusSeconds(10));

        assertThrows(IllegalStateException.class, () -> consumer.consume(collision));

        verifyNoInteractions(producer);
        assertEquals(first, repository.saveOrLoad(first).event());
        assertTrue(stored().get(PAYLOAD).s().contains("hello"));
    }

    @Test
    void concurrentDuplicatesKeepOnePersistedIngressTime() throws Exception {
        var start = new CountDownLatch(1);
        var results = new ArrayList<Future<DeliveryEvent>>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 20; i++) {
                var event = retryAt(first.occurredAt().plusSeconds(i));
                results.add(executor.submit(() -> {
                    start.await();
                    return repository.saveOrLoad(event).event();
                }));
            }
            start.countDown();
            var persistedTime = results.getFirst().get(15, TimeUnit.SECONDS).occurredAt();
            for (var result : results) {
                assertEquals(persistedTime, result.get(15, TimeUnit.SECONDS).occurredAt());
            }
            assertEquals(persistedTime.toString(), stored().get(OCCURRED_AT).s());
        }
    }

    private DeliveryEvent retryAt(Instant time) {
        return DeliveryEvent.requested(first.deliveryId(), first.tenantId(), first.deliveryType(), first.payload(), time);
    }

    @Test
    void fallbackChoiceCannotChangeOnSameClientKeyAndLegacyRecordsMeanFalse() {
        repository.saveOrLoad(first);
        var changed = DeliveryEvent.requested(first.deliveryId(), first.tenantId(), first.deliveryType(),
                first.payload(), first.occurredAt(), true);
        assertThrows(IdempotencyConflictException.class, () -> repository.saveOrLoad(changed));
        client.updateItem(request -> request.tableName(tableForKey(key())).key(key()).updateExpression("REMOVE fallback_allowed"));
        assertFalse(repository.saveOrLoad(first).event().fallbackAllowed());
        assertThrows(IdempotencyConflictException.class, () -> repository.saveOrLoad(changed));
    }

    @Test
    void allowedFallbackSurvivesDuplicateAndRetainsOriginalTimestamp() {
        var allowed = DeliveryEvent.requested(first.deliveryId(), first.tenantId(), first.deliveryType(), first.payload(), first.occurredAt(), true);
        repository.saveOrLoad(allowed);
        var duplicate = DeliveryEvent.requested(first.deliveryId(), first.tenantId(), first.deliveryType(), first.payload(), first.occurredAt().plusSeconds(20), true);
        var actual = repository.saveOrLoad(duplicate).event().toDispatchRequested();
        assertTrue(actual.fallbackAllowed());
        assertEquals(first.occurredAt(), actual.occurredAt());
    }

    @Test void compactedSameRequestSkipsPublicationButChangedPayloadOrFallbackIsConflict() {
        var completion = new java.util.HashMap<>(key());
        completion.put(STATUS, AttributeValue.fromS("COMPLETED"));
        completion.put(EVENT_ID, AttributeValue.fromS(first.eventId()));
        completion.put("fingerprint_version", AttributeValue.fromN("1"));
        completion.put(event.common.lifecycle.DeliveryCompletion.FINGERPRINT, AttributeValue.fromS(
                event.common.lifecycle.DeliveryCompletion.fingerprint(first.deliveryId(), first.tenantId(), first.deliveryType(), false,
                        JsonMapper.builder().build().writeValueAsString(event.common.delivery.DeliveryPayloads.canonicalize(first.payload())))));
        client.putItem(r -> r.tableName(tableForKey(completion)).item(completion));
        consumer.consume(retryAt(first.occurredAt().plusSeconds(86400)));
        assertTrue(repository.saveOrLoad(first).completed());
        var changed = DeliveryEvent.requested(first.deliveryId(), first.tenantId(), first.deliveryType(), Map.of("body", "changed"), first.occurredAt());
        assertThrows(IdempotencyConflictException.class, () -> consumer.consume(changed));
        var fallback = DeliveryEvent.requested(first.deliveryId(), first.tenantId(), first.deliveryType(), first.payload(), first.occurredAt(), true);
        assertThrows(IdempotencyConflictException.class, () -> consumer.consume(fallback));
        verifyNoInteractions(producer);
        assertEquals(completion, stored());
    }

    @Test void finalizedButNotCompactedRequestAlsoSkipsNewDispatch() {
        repository.saveOrLoad(first);
        client.updateItem(r -> r.tableName(tableForKey(key())).key(key()).updateExpression("SET completion_event_id = :id")
                .expressionAttributeValues(Map.of(":id", AttributeValue.fromS(event.common.lifecycle.DeliveryFinalized.eventId(first.deliveryId())))));
        consumer.consume(retryAt(first.occurredAt().plusSeconds(30)));
        verifyNoInteractions(producer);
        assertTrue(stored().containsKey(PAYLOAD));
    }

    private Map<String, AttributeValue> key() {
        return Map.of(PK, AttributeValue.fromS("DELIVERY#" + first.deliveryId()), SK, AttributeValue.fromS("META"));
    }

    private Map<String, AttributeValue> stored() {
        return client.getItem(request -> request.tableName(tableForKey(key())).key(key()).consistentRead(true)).item();
    }
    private static event.common.metrics.DeliveryMetrics metrics() {
        return new event.common.metrics.DeliveryMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
    }

}

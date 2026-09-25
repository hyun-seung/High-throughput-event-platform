package event.delivery.dispatch.receipt;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryIds;
import event.common.dynamodb.config.DynamoDbTableInitializer;
import event.common.metrics.DeliveryMetrics;
import event.common.receipt.ReceiptEvent;
import event.common.receipt.ReceiptOutcome;
import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.config.SecondaryDispatchProperties;
import event.delivery.dispatch.external.client.ProviderFailureException;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import event.delivery.dispatch.model.DispatchAttempt;
import event.delivery.dispatch.port.DeliveryProviderClient;
import event.delivery.dispatch.port.SecondaryProviderClient;
import event.delivery.dispatch.repository.DispatchAttemptRepository;
import event.delivery.dispatch.service.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import event.delivery.dispatch.config.DispatchFailureConfiguration;

import static event.common.dynamodb.DynamoDbAttributeNames.*;
import static event.common.dynamodb.DynamoDbTableNames.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@EnabledIfEnvironmentVariable(named = "DYNAMODB_TEST_ENDPOINT", matches = ".+")
class ReceiptResultDynamoDbTest {
    static final Instant NOW = Instant.parse("2026-09-24T00:00:00Z");
    static final String PRIMARY = "mock-provider", SECONDARY = "tcp-provider";
    static final DispatchProperties CONFIG = new DispatchProperties(PRIMARY, Duration.ofSeconds(30));
    static DynamoDbClient db;
    final JsonMapper mapper = JsonMapper.builder().build();
    final List<Map<String, AttributeValue>> keys = new ArrayList<>();
    final AtomicInteger primaryCalls = new AtomicInteger(), secondaryCalls = new AtomicInteger();
    final MutableClock clock = new MutableClock();
    DispatchAttemptRepository attempts;
    ReceiptResultRepository results;
    ReceiptResultService service;
    DeliveryEvent event;

    @BeforeAll static void connect() {
        URI endpoint = URI.create(System.getenv("DYNAMODB_TEST_ENDPOINT"));
        if (!"http".equals(endpoint.getScheme()) || !Set.of("localhost", "127.0.0.1").contains(endpoint.getHost()))
            throw new IllegalArgumentException("Tests require local DynamoDB");
        db = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(10))).build();
        new DynamoDbTableInitializer(db).run(null);
    }

    @BeforeEach void setUp() {
        attempts = new DispatchAttemptRepository(db);
        results = new ReceiptResultRepository(db, CONFIG, mapper);
        service = service(results);
        event = create(true);
    }

    @AfterEach void cleanup() {
        keys.forEach(key -> db.deleteItem(r -> r.tableName(tableForKey(key)).key(key)));
    }
    @AfterAll static void close() { if (db != null) db.close(); }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {1, 2})
    void successBeforeProviderResponseFencesBothLaterOutcomesAndPreventsResend(int route) {
        var attempt = processingRoute(event, route);
        var receipt = receipt(event, route, "DELIVERED", 1);
        service.process(receipt);
        assertEquals("DELIVERED", stored(event, route).get(STATUS).s());
        assertThrows(ConditionalCheckFailedException.class, () -> attempts.markAccepted(attempt, NOW, NOW));
        assertThrows(ConditionalCheckFailedException.class, () -> attempts.recordFailure(attempt,
                DispatchRetryPolicy.decide(attempt, ProviderFailureException.Kind.FALLBACK_REQUIRED, NOW, CONFIG)));
        service.process(receipt);
        primaryService().dispatch(event);
        assertEquals(0, primaryCalls.get());
        assertEquals(0, secondaryCalls.get());
        assertEquals("2", stored(event, route).get(VERSION).n());
    }

    @Test void legacyFirstInvocationAndPrimaryDeadlineAreStillReadable() {
        accepted(event);
        db.updateItem(r -> r.tableName(tableForKey(ReceiptResultRepository.attemptKey(event.deliveryId(), id(event, 1)))).key(ReceiptResultRepository.attemptKey(event.deliveryId(), id(event, 1)))
                .updateExpression("SET primary_deadline = :deadline REMOVE deadline_at, route_order")
                .expressionAttributeValues(Map.of(":deadline", AttributeValue.fromN(Long.toString(NOW.plus(Duration.ofHours(3)).toEpochMilli())))));
        service.process(receipt(event, 1, "DELIVERED", null));
        assertEquals("DELIVERED", stored(event, 1).get(STATUS).s());
    }

    @Test void fallbackFailureAndReplaySendSecondaryOnceWithOriginalDecisionDeadline() {
        accepted(event);
        var receipt = receipt(event, 1, "FALLBACK", 1);
        service.process(receipt);
        clock.now = NOW.plusSeconds(10);
        var repeat = ReceiptEvent.received(receipt.receiptId(), receipt.deliveryId(), receipt.attemptId(), receipt.provider(),
                1, receipt.outcome(), receipt.code(), receipt.occurredAt(), clock.now, 1);
        service.process(repeat);
        assertEquals(1, secondaryCalls.get());
        assertEquals("ACCEPTED", stored(event, 2).get(STATUS).s());
        assertEquals(Long.toString(NOW.plus(Duration.ofHours(4)).toEpochMilli()), stored(event, 2).get(DEADLINE_AT).n());
    }

    @Test void fallbackDisabledNeverSendsSecondary() {
        var denied = create(false);
        accepted(denied);
        service.process(receipt(denied, 1, "FALLBACK", 1));
        assertEquals(0, secondaryCalls.get());
        assertEquals("DECISION_PENDING", stored(denied, 1).get(STATUS).s());
        assertTrue(stored(denied, 2).isEmpty());
    }

    @Test void retryReceiptResumesAfterDueAndOldReceiptCannotCreateAnotherRetry() {
        accepted(event);
        var first = receipt(event, 1, "RETRY_1S", 1);
        assertThrows(DispatchRetryPendingException.class, () -> service.process(first));
        assertEquals(0, primaryCalls.get());
        clock.now = NOW.plusSeconds(1);
        service.process(first);
        service.process(first);
        assertEquals(1, primaryCalls.get());
        assertEquals("1", stored(event, 1).get(RETRY_COUNT).n());
        assertEquals("ACCEPTED", stored(event, 1).get(STATUS).s());
        var stale = receipt(event, 1, "FALLBACK", 1);
        assertEquals("stale_invocation", results.apply(stale, clock.now).outcome());
        assertEquals(0, secondaryCalls.get());
        assertTrue(ledger(stale).isEmpty());
    }

    @Test void receiptRetriesStopAfterThreeWithoutExtendingDeadline() {
        accepted(event);
        for (int invocation = 1; invocation <= 4; invocation++) {
            var receipt = receipt(event, 1, "RETRY_1S", invocation);
            if (invocation < 4) {
                assertThrows(DispatchRetryPendingException.class, () -> service.process(receipt));
                clock.now = clock.now.plusSeconds(1);
                service.process(receipt);
            } else service.process(receipt);
        }
        assertEquals(3, primaryCalls.get());
        var stored = stored(event, 1);
        assertEquals("DECISION_PENDING", stored.get(STATUS).s());
        assertEquals("RETRY_EXHAUSTED_RETRY_1S", stored.get(FAILURE_REASON).s());
        assertEquals(Long.toString(NOW.plus(Duration.ofHours(3)).toEpochMilli()), stored.get(DEADLINE_AT).n());
    }

    @Test void lateAndQueuedPastDeadlineReceiptsLeaveNoBusinessOrDedupeWrites() {
        accepted(event);
        var original = stored(event, 1);
        clock.now = NOW.plus(Duration.ofHours(3));
        var late = receipt(event, 1, "DELIVERED", 1);
        assertEquals("late_discarded", results.apply(late, clock.now).outcome());
        var queued = ReceiptEvent.received(UUID.randomUUID().toString(), event.deliveryId(), id(event, 1), PRIMARY, 1,
                ReceiptOutcome.DELIVERED, "DELIVERED", NOW, NOW.plusSeconds(1), 1);
        track(queued);
        assertEquals("late_discarded", results.apply(queued, clock.now).outcome());
        assertEquals(original, stored(event, 1));
        assertTrue(ledger(late).isEmpty());
        assertTrue(ledger(queued).isEmpty());
    }

    @Test void latePrimaryDoesNotChangeSecondaryAndSecondarySuccessFinalizesOnlySecondary() {
        accepted(event);
        service.process(receipt(event, 1, "FALLBACK", 1));
        clock.now = NOW.plus(Duration.ofHours(3));
        var child = stored(event, 2);
        service.process(receipt(event, 1, "DELIVERED", 1));
        assertEquals(child, stored(event, 2));
        service.process(receipt(event, 2, "DELIVERED", 1));
        assertEquals("DELIVERED", stored(event, 2).get(STATUS).s());
        assertEquals("DECISION_PENDING", stored(event, 1).get(STATUS).s());
        assertEquals(1, secondaryCalls.get());
    }

    @Test void secondaryReceiptRetryUsesSameBoundRouteAndDoesNotCreateThirdRoute() {
        accepted(event);
        service.process(receipt(event, 1, "FALLBACK", 1));
        var retry = receipt(event, 2, "RETRY_1S", 1);
        assertThrows(DispatchRetryPendingException.class, () -> service.process(retry));
        clock.now = NOW.plusSeconds(1);
        service.process(retry);
        service.process(retry);
        assertEquals(2, secondaryCalls.get());
        assertEquals("1", stored(event, 2).get(RETRY_COUNT).n());
        service.process(receipt(event, 2, "FALLBACK", 2));
        assertEquals("DECISION_PENDING", stored(event, 2).get(STATUS).s());
        assertEquals(2, secondaryCalls.get());
    }

    @Test void validReceiptCanResolveLeaseReviewButUnknownFailureStaysForReview() {
        claim(event);
        clock.now = NOW.plusSeconds(31);
        primaryService().dispatch(event);
        assertEquals("REVIEW_REQUIRED", stored(event, 1).get(STATUS).s());
        service.process(receipt(event, 1, "DELIVERED", 1));
        assertEquals("DELIVERED", stored(event, 1).get(STATUS).s());
        var other = create(false);
        accepted(other);
        service.process(receipt(other, 1, "NEW_CODE", 1));
        assertEquals("REVIEW_REQUIRED", stored(other, 1).get(STATUS).s());
        assertEquals(0, primaryCalls.get());
    }

    @Test void httpRetryAndFailureReceiptDoNotScheduleTwiceButSuccessCanCancelUnsentRetry() {
        var attempt = claim(event);
        attempts.recordFailure(attempt, DispatchRetryPolicy.decide(attempt, ProviderFailureException.Kind.RETRY_1S, NOW, CONFIG));
        var original = stored(event, 1);
        assertEquals("retry_already_scheduled", results.apply(receipt(event, 1, "RETRY_1S", 1), NOW).outcome());
        assertEquals(original, stored(event, 1));
        service.process(receipt(event, 1, "DELIVERED", 1));
        clock.now = NOW.plusSeconds(1);
        primaryService().dispatch(event);
        assertEquals(0, primaryCalls.get());
    }

    @Test void globalReceiptIdConflictCannotModifyAnotherDelivery() {
        accepted(event);
        var receipt = receipt(event, 1, "DELIVERED", 1);
        service.process(receipt);
        var other = create(true);
        accepted(other);
        var conflict = ReceiptEvent.received(receipt.receiptId(), other.deliveryId(), id(other, 1), PRIMARY, 1,
                ReceiptOutcome.FAILED, "FALLBACK", NOW, NOW, 1);
        assertThrows(IllegalStateException.class, () -> service.process(conflict));
        assertEquals("ACCEPTED", stored(other, 1).get(STATUS).s());
        assertEquals(0, secondaryCalls.get());
    }

    @Test void transactionResponseLossReplaysCommittedHandoffWithoutRepeatingStateChange() {
        accepted(event);
        var receipt = receipt(event, 1, "FALLBACK", 1);
        var uncertain = mock(DynamoDbClient.class, delegatesTo(db));
        doAnswer(call -> {
            db.transactWriteItems(call.getArgument(0, TransactWriteItemsRequest.class));
            throw SdkClientException.create("transaction response lost");
        }).when(uncertain).transactWriteItems(any(TransactWriteItemsRequest.class));
        var interrupted = new ReceiptResultRepository(uncertain, CONFIG, mapper);
        assertThrows(SdkClientException.class, () -> service(interrupted).process(receipt));
        assertFalse(ledger(receipt).isEmpty());
        service.process(receipt);
        service.process(receipt);
        assertEquals("2", stored(event, 1).get(VERSION).n());
        assertEquals(1, secondaryCalls.get());
    }

    @Test void concurrentDuplicateReceiptsHaveOneAtomicStateChange() throws Exception {
        accepted(event);
        var receipt = receipt(event, 1, "DELIVERED", 1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new ArrayList<Future<?>>();
            for (int i = 0; i < 16; i++) futures.add(executor.submit(() -> {
                for (int retry = 0; retry < 10; retry++) {
                    try { service.process(receipt); return; }
                    catch (IllegalStateException | TransactionConflictException race) {
                        if (retry == 9) throw race;
                    }
                }
            }));
            for (var future : futures) future.get(20, TimeUnit.SECONDS);
        }
        assertEquals("DELIVERED", stored(event, 1).get(STATUS).s());
        assertEquals("2", stored(event, 1).get(VERSION).n());
        assertFalse(ledger(receipt).isEmpty());
    }

    @Test void futureAndAmbiguousLegacyInvocationAreRetainedWithoutChangingState() {
        accepted(event);
        assertThrows(IllegalStateException.class, () -> service.process(receipt(event, 1, "DELIVERED", 2)));
        var retry = receipt(event, 1, "RETRY_1S", 1);
        assertThrows(DispatchRetryPendingException.class, () -> service.process(retry));
        clock.now = NOW.plusSeconds(1);
        service.process(retry);
        var ambiguous = receipt(event, 1, "DELIVERED", null);
        var original = stored(event, 1);
        assertThrows(IllegalStateException.class, () -> service.process(ambiguous));
        assertEquals(original, stored(event, 1));
        assertTrue(ledger(ambiguous).isEmpty());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "KAFKA_TEST_BOOTSTRAP_SERVERS", matches = ".+")
    void consumerRestartRetainsOffsetAfterDecisionAndCompletesSecondaryHandoff() throws Exception {
        String bootstrap = System.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS");
        for (String address : bootstrap.split(",")) {
            if (!address.trim().matches("(localhost|127\\.0\\.0\\.1):[0-9]+")) throw new IllegalArgumentException("Local Kafka required");
        }
        String topic = "test.receipt-result." + UUID.randomUUID();
        String group = topic + ".worker";
        var partition = new TopicPartition(topic, 0);
        try (var admin = Admin.create(Map.of("bootstrap.servers", bootstrap, "request.timeout.ms", 5000, "default.api.timeout.ms", 10000));
             var producer = new KafkaProducer<String, byte[]>(Map.of("bootstrap.servers", bootstrap, "acks", "all"),
                     new StringSerializer(), new ByteArraySerializer())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            var kafka = new KafkaProperties();
            kafka.setBootstrapServers(List.of(bootstrap.split(",")));
            kafka.getConsumer().setAutoOffsetReset("earliest");
            kafka.getConsumer().setKeyDeserializer(ErrorHandlingDeserializer.class);
            kafka.getConsumer().setValueDeserializer(ErrorHandlingDeserializer.class);
            kafka.getConsumer().getProperties().putAll(Map.of(
                    "spring.deserializer.key.delegate.class", StringDeserializer.class.getName(),
                    "spring.deserializer.value.delegate.class", JacksonJsonDeserializer.class.getName()));
            var factory = new ReceiptConsumerConfiguration().receiptListenerContainerFactory(kafka,
                    new DispatchFailureConfiguration().dispatchErrorHandler(100), 1);
            var baseline = create(false);
            accepted(baseline); accepted(event);
            var baselineReceipt = receipt(baseline, 1, "DELIVERED", 1);
            var blocked = receipt(event, 1, "FALLBACK", 1);
            var injected = new AtomicBoolean(true);
            var decisionSaved = new CountDownLatch(1);
            var repository = spy(results);
            doAnswer(call -> {
                var r = call.getArgument(0, ReceiptEvent.class);
                var applied = results.apply(r, call.getArgument(1, Instant.class));
                if (r.eventId().equals(blocked.eventId()) && injected.get()) {
                    decisionSaved.countDown();
                    throw new IllegalStateException("Stopped after durable decision before dispatch handoff");
                }
                return applied;
            }).when(repository).apply(any(), any());
            var handler = new ReceiptResultConsumer(service(repository));
            ConcurrentMessageListenerContainer<String, ReceiptEvent> container = factory.createContainer(topic);
            container.getContainerProperties().setGroupId(group);
            container.getContainerProperties().setPollTimeout(100);
            container.getContainerProperties().setMessageListener((MessageListener<String, ReceiptEvent>) r -> handler.consume(r.value()));
            try {
                container.start();
                producer.send(new ProducerRecord<>(topic, baseline.deliveryId(), mapper.writeValueAsBytes(baselineReceipt))).get(10, TimeUnit.SECONDS);
                awaitOffset(admin, group, partition, 1);
                producer.send(new ProducerRecord<>(topic, event.deliveryId(), mapper.writeValueAsBytes(blocked))).get(10, TimeUnit.SECONDS);
                assertTrue(decisionSaved.await(10, TimeUnit.SECONDS));
                assertEquals(1L, offset(admin, group, partition));
                assertEquals(0, secondaryCalls.get());
                container.stop();
                injected.set(false);
                // New container/service, same persisted state and consumer group; no offset reset.
                container = factory.createContainer(topic);
                container.getContainerProperties().setGroupId(group);
                container.getContainerProperties().setPollTimeout(100);
                var recovered = new ReceiptResultConsumer(service(results));
                container.getContainerProperties().setMessageListener((MessageListener<String, ReceiptEvent>) r -> recovered.consume(r.value()));
                container.start();
                awaitOffset(admin, group, partition, 2);
                assertEquals(1, secondaryCalls.get());
                assertEquals("ACCEPTED", stored(event, 2).get(STATUS).s());
            } finally {
                container.stop();
                admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
                admin.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
            }
        }
    }

    static long offset(Admin admin, String group, TopicPartition partition) throws Exception {
        var value = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(partition);
        return value == null ? -1 : value.offset();
    }
    static void awaitOffset(Admin admin, String group, TopicPartition partition, long expected) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            if (offset(admin, group, partition) == expected) return;
            Thread.sleep(25);
        }
        assertEquals(expected, offset(admin, group, partition));
    }

    ReceiptResultService service(ReceiptResultRepository repository) {
        var metrics = new DeliveryMetrics(new SimpleMeterRegistry());
        SecondaryProviderClient provider = (e, key) -> {
            secondaryCalls.incrementAndGet();
            return new ProviderDispatchResponse(e.deliveryId(), true, clock.instant());
        };
        var secondary = new SecondaryDispatchService(attempts, provider,
                new SecondaryDispatchProperties(SECONDARY, Duration.ofHours(4)), CONFIG, clock, metrics);
        return new ReceiptResultService(repository, primaryService(secondary, metrics), secondary, CONFIG, clock, metrics);
    }

    DispatchService primaryService() {
        var metrics = new DeliveryMetrics(new SimpleMeterRegistry());
        return primaryService(new SecondaryDispatchService(attempts, (e, key) -> {
            secondaryCalls.incrementAndGet(); return new ProviderDispatchResponse(e.deliveryId(), true, clock.instant());
        }, new SecondaryDispatchProperties(SECONDARY, Duration.ofHours(4)), CONFIG, clock, metrics), metrics);
    }

    DispatchService primaryService(SecondaryDispatchService secondary, DeliveryMetrics metrics) {
        return new DispatchService(attempts, (e, key) -> {
            primaryCalls.incrementAndGet(); return new ProviderDispatchResponse(e.deliveryId(), true, clock.instant());
        }, CONFIG, clock, metrics, secondary);
    }

    DeliveryEvent create(boolean allowed) {
        var result = DeliveryEvent.requested(UUID.randomUUID().toString(), 999L, "SMS", Map.of("message", "test"), NOW, allowed).toDispatchRequested();
        var metaKey = Map.of(PK, AttributeValue.fromS("DELIVERY#" + result.deliveryId()), SK, AttributeValue.fromS("META"));
        var item = new HashMap<>(metaKey);
        item.put("delivery_id", AttributeValue.fromS(result.deliveryId()));
        item.put(TENANT_ID, AttributeValue.fromN("999")); item.put(DELIVERY_TYPE, AttributeValue.fromS("SMS"));
        item.put(PAYLOAD, AttributeValue.fromS(mapper.writeValueAsString(result.payload())));
        item.put(OCCURRED_AT, AttributeValue.fromS(NOW.toString())); item.put(FALLBACK_ALLOWED, AttributeValue.fromBool(allowed));
        db.putItem(r -> r.tableName(tableForKey(item)).item(item));
        keys.add(metaKey);
        keys.add(ReceiptResultRepository.attemptKey(result.deliveryId(), id(result, 1)));
        keys.add(ReceiptResultRepository.attemptKey(result.deliveryId(), id(result, 2)));
        return result;
    }

    DispatchAttempt claim(DeliveryEvent e) {
        return attempts.claim(e, id(e, 1), PRIMARY, NOW, NOW.plusSeconds(30), NOW.plus(Duration.ofHours(3))).attempt();
    }
    DispatchAttempt processingRoute(DeliveryEvent e, int route) {
        var primary = claim(e);
        if (route == 1) return primary;
        attempts.recordFailure(primary, DispatchRetryPolicy.decide(primary, ProviderFailureException.Kind.FALLBACK_REQUIRED, NOW, CONFIG));
        var bound = attempts.prepareSecondary(e, id(e, 1), SECONDARY, Duration.ofHours(4)).orElseThrow();
        return attempts.claimSecondary(e, bound, NOW, NOW.plusSeconds(30)).attempt();
    }
    void accepted(DeliveryEvent e) { attempts.markAccepted(claim(e), NOW, NOW); }
    static String id(DeliveryEvent e, int route) { return DeliveryIds.attemptId(e.deliveryId(), route == 1 ? PRIMARY : SECONDARY, route, 1); }
    ReceiptEvent receipt(DeliveryEvent e, int route, String code, Integer invocation) {
        var result = ReceiptEvent.received(UUID.randomUUID().toString(), e.deliveryId(), id(e, route), route == 1 ? PRIMARY : SECONDARY,
                route, code.equals("DELIVERED") ? ReceiptOutcome.DELIVERED : ReceiptOutcome.FAILED, code, clock.instant(), clock.instant(), invocation);
        track(result); return result;
    }
    void track(ReceiptEvent receipt) { keys.add(ReceiptResultRepository.receiptKey(receipt.eventId())); }
    Map<String, AttributeValue> ledger(ReceiptEvent receipt) { return read(ReceiptResultRepository.receiptKey(receipt.eventId())); }
    Map<String, AttributeValue> stored(DeliveryEvent e, int route) { return read(ReceiptResultRepository.attemptKey(e.deliveryId(), id(e, route))); }
    Map<String, AttributeValue> read(Map<String, AttributeValue> key) { return db.getItem(r -> r.tableName(tableForKey(key)).key(key).consistentRead(true)).item(); }

    static class MutableClock extends Clock {
        volatile Instant now = NOW;
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
        public Instant instant() { return now; }
    }
}

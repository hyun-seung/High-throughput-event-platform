package event.delivery.dispatch.lifecycle;

import event.common.delivery.*;
import event.common.lifecycle.*;
import event.common.dynamodb.config.DynamoDbTableInitializer;
import event.common.metrics.DeliveryMetrics;
import event.common.receipt.*;
import event.delivery.dispatch.config.*;
import event.delivery.dispatch.external.client.ProviderFailureException;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import event.delivery.dispatch.model.*;
import event.delivery.dispatch.receipt.ReceiptResultRepository;
import event.delivery.dispatch.repository.DispatchAttemptRepository;
import event.delivery.dispatch.service.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static event.common.dynamodb.DynamoDbTableNames.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@EnabledIfEnvironmentVariable(named = "DYNAMODB_TEST_ENDPOINT", matches = ".+")
class LifecycleDynamoDbTest {
    static DynamoDbClient db;
    static final Instant NOW = Instant.parse("2026-09-25T00:00:00Z");
    static final String PRIMARY = "mock-provider", SECONDARY = "tcp-provider";
    final JsonMapper mapper = JsonMapper.builder().build();
    final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    final DispatchProperties config = new DispatchProperties(PRIMARY, Duration.ofSeconds(1), Duration.ofSeconds(3), Duration.ofSeconds(1));
    final MutableClock clock = new MutableClock();
    final List<DeliveryEvent> events = new ArrayList<>();
    final List<String> receipts = new ArrayList<>();
    final List<DeliveryFinalized> published = new CopyOnWriteArrayList<>();
    final AtomicInteger primaryCalls = new AtomicInteger(), secondaryCalls = new AtomicInteger();
    DispatchAttemptRepository attempts;
    ReceiptResultRepository sources;
    LifecycleRepository lifecycle;
    DispatchService dispatch;
    SecondaryDispatchService secondary;
    LifecycleService service;

    @BeforeAll static void connect() {
        var uri = URI.create(System.getenv("DYNAMODB_TEST_ENDPOINT"));
        if (!Set.of("localhost", "127.0.0.1").contains(uri.getHost())) throw new IllegalArgumentException("Local only");
        db = DynamoDbClient.builder().endpointOverride(uri).region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
                .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(10))).build();
        new DynamoDbTableInitializer(db).run(null);
    }
    @BeforeEach void setup() {
        attempts = new DispatchAttemptRepository(db);
        sources = new ReceiptResultRepository(db, config, mapper);
        lifecycle = new LifecycleRepository(db, mapper);
        var stages = new DeliveryMetrics(metrics);
        secondary = new SecondaryDispatchService(attempts, (event, id) -> {
            secondaryCalls.incrementAndGet(); return new ProviderDispatchResponse(event.deliveryId(), true, clock.instant());
        }, new SecondaryDispatchProperties(SECONDARY, Duration.ofSeconds(5)), config, clock, stages);
        dispatch = new DispatchService(attempts, (event, id) -> {
            primaryCalls.incrementAndGet(); return new ProviderDispatchResponse(event.deliveryId(), true, clock.instant());
        }, config, clock, stages, secondary);
        service = service(lifecycle, published::add);
    }
    @AfterEach void cleanup() {
        for (var event : events) {
            for (String sk : List.of("META", "FINAL", "ATTEMPT#" + id(event, 1), "ATTEMPT#" + id(event, 2)))
                db.deleteItem(r -> r.tableName(tableForKey(LifecycleRepository.key(event.deliveryId(), sk))).key(LifecycleRepository.key(event.deliveryId(), sk)));
        }
        receipts.forEach(id -> db.deleteItem(r -> r.tableName(tableForKey(ReceiptResultRepository.receiptKey(id))).key(ReceiptResultRepository.receiptKey(id))));
        metrics.close();
    }
    @AfterAll static void close() { db.close(); }

    @Test void expiryIsInclusiveAndOnlyAllowedFallbackUsesOriginalDeadline() {
        var event = event(true); accepted(event);
        clock.now = NOW.plusMillis(2999); service.reconcile(event.deliveryId());
        assertTrue(published.isEmpty()); assertEquals(0, secondaryCalls.get());
        clock.now = NOW.plusSeconds(3); service.reconcile(event.deliveryId());
        assertEquals("DECISION_PENDING", state(event, 1)); assertEquals("ACCEPTED", state(event, 2));
        assertEquals(NOW.plusSeconds(8), LifecycleRepository.deadline(item(event, 2)));
        assertEquals(1, secondaryCalls.get());
        clock.now = NOW.plusSeconds(8); service.reconcile(event.deliveryId());
        assertEquals("EXPIRED", published.getFirst().outcome()); assertEquals(2, published.getFirst().routeOrder());
        assertEquals(NOW.plusSeconds(8), published.getFirst().resultAt());
        assertEquals(1, secondaryCalls.get());
        assertFalse(item(event, 1).containsKey(LifecycleIndex.BUCKET));
        assertFalse(item(event, 2).containsKey(LifecycleIndex.BUCKET));
    }

    @Test void noFallbackExpiryNeverCreatesSecondaryAndFencesLateSender() {
        var event = event(false); var pending = claim(event);
        clock.now = NOW.plusSeconds(3); service.reconcile(event.deliveryId());
        assertEquals("EXPIRED", published.getFirst().outcome()); assertTrue(item(event, 2).isEmpty());
        assertThrows(ConditionalCheckFailedException.class, () -> attempts.markAccepted(pending, NOW, clock.instant()));
        dispatch.dispatch(event); service.reconcile(event.deliveryId());
        assertEquals(0, primaryCalls.get()); assertEquals(1, published.size());
    }

    @Test void longOutageExpiresBothStagesWithoutExternalCallsOrDeadlineReset() {
        var event = event(true); accepted(event);
        clock.now = NOW.plusSeconds(100); service.reconcile(event.deliveryId());
        assertEquals(0, primaryCalls.get()); assertEquals(0, secondaryCalls.get());
        assertEquals("SECONDARY_EXPIRED", published.getFirst().reason());
        assertEquals(NOW.plusSeconds(8), published.getFirst().deadline());
    }

    @Test void reviewRequiredWaitsUntilDeadlineAndCannotPostponeFinalization() {
        var event = event(false); claim(event);
        clock.now = NOW.plusSeconds(2);
        assertEquals(DispatchClaimStatus.REVIEW_REQUIRED, attempts.claim(event, id(event, 1), PRIMARY,
                clock.instant(), clock.instant().plusSeconds(1), NOW.plusSeconds(3)).status());
        service.reconcile(event.deliveryId()); assertTrue(published.isEmpty());
        clock.now = NOW.plusSeconds(3); service.reconcile(event.deliveryId());
        assertEquals("EXPIRED", published.getFirst().outcome()); assertEquals(0, primaryCalls.get());
    }

    @Test void successBeforeExpiryIsPreservedAfterLongOutageAndLateReceipt() {
        var event = event(true); accepted(event); success(event, 1);
        clock.now = NOW.plusSeconds(100); service.reconcile(event.deliveryId());
        assertEquals("DELIVERED", published.getFirst().outcome()); assertEquals(0, secondaryCalls.get());
        var snapshot = item(event, 1); success(event, 1); service.reconcile(event.deliveryId());
        assertEquals(snapshot, item(event, 1)); assertEquals(1, published.size());
    }

    @Test void terminalFailureFinalizesWithoutEnablingUnrequestedFallback() {
        var event = event(true); var attempt = claim(event);
        attempts.recordFailure(attempt, DispatchRetryPolicy.decide(attempt, ProviderFailureException.Kind.PERMANENT_REJECTION, NOW, config));
        service.reconcile(event.deliveryId());
        assertEquals("FAILED", published.getFirst().outcome()); assertEquals("PERMANENT_REJECTION", published.getFirst().reason());
        assertEquals(0, secondaryCalls.get());
    }

    @Test void metaOnlyRecoveryStartsMissingDispatchAndReleasesSourceIndex() {
        var event = event(false); service.reconcile(event.deliveryId());
        assertEquals(1, primaryCalls.get()); assertEquals("ACCEPTED", state(event, 1));
        assertFalse(lifecycle.read(event.deliveryId(), "META").containsKey(LifecycleIndex.BUCKET));
        assertTrue(item(event, 1).containsKey(LifecycleIndex.BUCKET));
        service.reconcile(event.deliveryId()); assertEquals(1, primaryCalls.get());
    }

    @Test void metaOnlyRecoveryAfterTotalDeadlineDoesNotSend() {
        var event = event(true); clock.now = NOW.plusSeconds(100);
        service.reconcile(event.deliveryId());
        assertEquals("EXPIRED", published.getFirst().outcome());
        assertEquals(0, primaryCalls.get()); assertEquals(0, secondaryCalls.get());
    }

    @Test void scheduledRetryCanBeRecoveredWithoutKafkaInput() {
        var event = event(false); var attempt = claim(event);
        attempts.recordFailure(attempt, DispatchRetryPolicy.decide(attempt, ProviderFailureException.Kind.RETRY_1S, NOW, config));
        assertThrows(DispatchRetryPendingException.class, () -> service.reconcile(event.deliveryId()));
        clock.now = NOW.plusSeconds(1); service.reconcile(event.deliveryId());
        assertEquals(1, primaryCalls.get()); assertEquals("1", item(event, 1).get("retry_count").n());
        assertEquals(NOW.plusSeconds(3), LifecycleRepository.deadline(item(event, 1)));
    }

    @Test void publishFailureRetainsFinalOutboxAndFreshServiceResumesSameBody() {
        var event = event(false); accepted(event); success(event, 1);
        var broken = service(lifecycle, result -> { published.add(result); throw new IllegalStateException("ack lost"); });
        assertThrows(IllegalStateException.class, () -> broken.reconcile(event.deliveryId()));
        var pending = lifecycle.read(event.deliveryId(), "FINAL");
        assertEquals("PENDING", pending.get("publish_state").s()); assertTrue(pending.containsKey(LifecycleIndex.BUCKET));
        clock.now = NOW.plusSeconds(100);
        service(lifecycle, published::add).reconcile(event.deliveryId());
        assertEquals(2, published.size()); assertEquals(published.get(0), published.get(1));
        var done = lifecycle.read(event.deliveryId(), "FINAL");
        assertEquals("PUBLISHED", done.get("publish_state").s()); assertFalse(done.containsKey(LifecycleIndex.BUCKET));
    }

    @Test void committedFinalTransactionWithLostResponseRemainsRecoverable() {
        var event = event(false); accepted(event); success(event, 1);
        var uncertain = spy(lifecycle);
        doAnswer(call -> { call.callRealMethod(); throw SdkClientException.create("lost transaction response"); })
                .when(uncertain).finalizeDelivery(any(), anyMap(), anyMap(), any());
        assertThrows(SdkClientException.class, () -> service(uncertain, published::add).reconcile(event.deliveryId()));
        assertTrue(published.isEmpty());
        service(lifecycle, published::add).reconcile(event.deliveryId());
        assertEquals(1, published.size());
    }

    @Test void failureAfterKafkaAckBeforePublishedMarkReplaysSameFinalEvent() {
        var event = event(false); accepted(event); success(event, 1);
        var uncertain = spy(lifecycle);
        doThrow(SdkClientException.create("mark unavailable")).when(uncertain).published(anyString(), anyString(), any());
        assertThrows(SdkClientException.class, () -> service(uncertain, published::add).reconcile(event.deliveryId()));
        service.reconcile(event.deliveryId()); assertEquals(2, published.size());
        assertEquals(published.get(0), published.get(1));
    }

    @Test void concurrentFinalizersKeepOneImmutableResult() throws Exception {
        var event = event(false); accepted(event); success(event, 1);
        try (var pool = Executors.newFixedThreadPool(8)) {
            var futures = new ArrayList<Future<?>>();
            for (int i = 0; i < 16; i++) futures.add(pool.submit(() -> service.reconcile(event.deliveryId())));
            for (var future : futures) future.get(15, TimeUnit.SECONDS);
        }
        service.reconcile(event.deliveryId());
        assertFalse(published.isEmpty()); assertEquals(1, new HashSet<>(published).size());
        assertEquals("PUBLISHED", lifecycle.read(event.deliveryId(), "FINAL").get("publish_state").s());
    }

    @Test void secondarySuccessSurvivesLatePrimaryAndGlobalDeadline() {
        var event = event(true); accepted(event); clock.now = NOW.plusSeconds(3);
        service.reconcile(event.deliveryId()); success(event, 2);
        clock.now = NOW.plusSeconds(100); success(event, 1); service.reconcile(event.deliveryId());
        assertEquals("DELIVERED", published.getFirst().outcome()); assertEquals(2, published.getFirst().routeOrder());
    }

    @Test void expiredSecondaryProcessingLeaseCannotBlockFinalization() {
        var event = event(true); var parent = claim(event);
        attempts.recordFailure(parent, DispatchRetryPolicy.expired(NOW.plusSeconds(3)));
        var route = attempts.prepareSecondary(event, id(event, 1), SECONDARY, Duration.ofSeconds(5)).orElseThrow();
        var oldSender = attempts.claimSecondary(event, route, NOW, NOW.plusSeconds(100)).attempt();
        clock.now = NOW.plusSeconds(8); service.reconcile(event.deliveryId());
        assertEquals("EXPIRED", published.getFirst().outcome());
        assertThrows(ConditionalCheckFailedException.class, () -> attempts.markAccepted(oldSender, NOW, NOW));
    }

    @Test void changedPrimaryConfigurationCannotCreateANewRouteDuringRecovery() {
        var event = event(false);
        String other = DeliveryIds.attemptId(event.deliveryId(), "other-provider", 1, 1);
        attempts.claim(event, other, "other-provider", NOW, NOW.plusSeconds(1), NOW.plusSeconds(3));
        try {
            assertThrows(IllegalStateException.class, () -> service.reconcile(event.deliveryId()));
            assertEquals(0, primaryCalls.get());
        } finally { db.deleteItem(r -> r.tableName(tableForKey(LifecycleRepository.key(event.deliveryId(), "ATTEMPT#" + other))).key(LifecycleRepository.key(event.deliveryId(), "ATTEMPT#" + other))); }
    }

    @Test void staleExpirySnapshotCannotOverwriteReceiptSuccess() {
        var event = event(false); accepted(event); var before = item(event, 1);
        success(event, 1); clock.now = NOW.plusSeconds(3);
        assertFalse(lifecycle.expire(event.deliveryId(), before, clock.instant()));
        service.reconcile(event.deliveryId()); assertEquals("DELIVERED", published.getFirst().outcome());
    }

    @Test void sparseIndexFindsDueMetaAndTerminalAttemptWithRealPagination() throws Exception {
        lifecycle.requireIndex();
        var event = event(false); accepted(event); success(event, 1);
        int shard = Math.floorMod(event.deliveryId().hashCode(), LifecycleIndex.SHARDS);
        Set<String> found = new HashSet<>();
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (found.size() < 2 && System.nanoTime() < until) {
            for (int tableShard : List.of(shard, shard + LifecycleIndex.SHARDS)) {
                Map<String, AttributeValue> cursor = Map.of();
                do {
                    var page = lifecycle.due(tableShard, NOW, 1, cursor);
                    page.items().stream().filter(i -> i.get("pk").s().equals("DELIVERY#" + event.deliveryId()))
                            .forEach(i -> found.add(i.get("sk").s()));
                    cursor = page.lastEvaluatedKey();
                } while (!cursor.isEmpty());
            }
            if (found.size() < 2) Thread.sleep(20);
        }
        assertEquals(Set.of("META", "ATTEMPT#" + id(event, 1)), found);
    }

    @Test void expiredThenReceivedSuccessDoesNotReopenFinalResult() {
        var event = event(false); accepted(event);
        clock.now = NOW.plusSeconds(3); service.reconcile(event.deliveryId());
        var frozen = lifecycle.read(event.deliveryId(), "FINAL");
        success(event, 1); service.reconcile(event.deliveryId());
        assertEquals(frozen, lifecycle.read(event.deliveryId(), "FINAL"));
        assertEquals("EXPIRED", published.getFirst().outcome());
    }

    @Test void compactedPrimaryBlocksOldDispatchChangedProviderLateReceiptAndLateResultWrite() {
        var event = event(false); var originalAttempt = claim(event);
        attempts.markAccepted(originalAttempt, NOW, NOW); success(event, 1); service.reconcile(event.deliveryId());
        var result = published.getFirst();
        var compactor = new DeliveryCompactor(db);
        assertTrue(compactor.compact(result)); assertFalse(compactor.compact(result));
        var meta = lifecycle.read(event.deliveryId(), "META");
        assertEquals("COMPLETED", LifecycleRepository.text(meta, "status"));
        assertFalse(meta.containsKey("payload")); assertFalse(meta.containsKey("ttl"));
        assertFalse(meta.containsKey(LifecycleIndex.BUCKET));
        assertTrue(lifecycle.read(event.deliveryId(), "FINAL").isEmpty()); assertTrue(item(event, 1).isEmpty());
        dispatch.dispatch(event); service.reconcile(event.deliveryId());
        assertEquals(0, primaryCalls.get());
        var changed = attempts.claim(event, DeliveryIds.attemptId(event.deliveryId(), "changed", 1, 1), "changed", NOW, NOW.plusSeconds(1), NOW.plusSeconds(3));
        assertEquals(DispatchClaimStatus.ALREADY_ACCEPTED, changed.status());
        assertThrows(ConditionalCheckFailedException.class, () -> attempts.markAccepted(originalAttempt, NOW, NOW));
        var late = ReceiptEvent.received(UUID.randomUUID().toString(), event.deliveryId(), id(event, 1), PRIMARY, 1, ReceiptOutcome.DELIVERED, "DELIVERED", NOW, NOW, 1);
        assertEquals("late_discarded", sources.apply(late, NOW).outcome());
        assertTrue(db.getItem(r -> r.tableName(tableForKey(ReceiptResultRepository.receiptKey(late.eventId()))).key(ReceiptResultRepository.receiptKey(late.eventId())).consistentRead(true)).item().isEmpty());
        for (String receipt : receipts) assertTrue(db.getItem(r -> r.tableName(tableForKey(ReceiptResultRepository.receiptKey(receipt))).key(ReceiptResultRepository.receiptKey(receipt)).consistentRead(true)).item().isEmpty());
        assertEquals(0, db.query(r -> r.tableName(STEP).consistentRead(true).keyConditionExpression("pk = :pk")
                .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS("DELIVERY#" + event.deliveryId())))).count());
    }

    @Test void completionFenceStopsNewClaimsEvenBeforeSqlAllowsCompaction() {
        var event = event(false); accepted(event); success(event, 1); service.reconcile(event.deliveryId());
        var changed = attempts.claim(event, DeliveryIds.attemptId(event.deliveryId(), "new-provider", 1, 1), "new-provider", NOW, NOW.plusSeconds(1), NOW.plusSeconds(3));
        assertEquals(DispatchClaimStatus.ALREADY_ACCEPTED, changed.status());
        assertTrue(lifecycle.read(event.deliveryId(), "META").containsKey("payload"));
    }

    @Test void retryReceiptManifestDeletesAllMarkersNotOnlyLatestOne() {
        var event = event(false); accepted(event);
        var retry = ReceiptEvent.received(UUID.randomUUID().toString(), event.deliveryId(), id(event, 1), PRIMARY, 1, ReceiptOutcome.FAILED, "RETRY_1S", NOW, NOW, 1);
        receipts.add(retry.eventId()); sources.apply(retry, NOW);
        clock.now = NOW.plusSeconds(1); dispatch.dispatch(event);
        var delivered = ReceiptEvent.received(UUID.randomUUID().toString(), event.deliveryId(), id(event, 1), PRIMARY, 1, ReceiptOutcome.DELIVERED, "DELIVERED", clock.now, clock.now, 2);
        receipts.add(delivered.eventId()); sources.apply(delivered, clock.now); service.reconcile(event.deliveryId());
        assertEquals(2, item(event, 1).get(DeliveryCompletion.RECEIPT_IDS).ss().size());
        new DeliveryCompactor(db).compact(published.getFirst());
        for (String receipt : receipts) assertTrue(db.getItem(r -> r.tableName(tableForKey(ReceiptResultRepository.receiptKey(receipt))).key(ReceiptResultRepository.receiptKey(receipt)).consistentRead(true)).item().isEmpty());
    }

    @Test void bothRoutesCompactAndStaleSecondaryPlanCannotRecreateAttempt() {
        var event = event(true); accepted(event); clock.now = NOW.plusSeconds(3);
        service.reconcile(event.deliveryId());
        var route = attempts.prepareSecondary(event, id(event, 1), SECONDARY, Duration.ofSeconds(5)).orElseThrow();
        success(event, 2); service.reconcile(event.deliveryId());
        new DeliveryCompactor(db).compact(published.getFirst());
        secondary.dispatch(event, id(event, 1));
        assertEquals(DispatchClaimStatus.ALREADY_ACCEPTED, attempts.claimSecondary(event, route, clock.now, clock.now.plusSeconds(1)).status());
        assertTrue(item(event, 1).isEmpty()); assertTrue(item(event, 2).isEmpty());
        assertEquals(1, secondaryCalls.get());
    }

    @Test void unpublishedOrLegacyUntrackedExecutionIsRetained() {
        var event = event(false); accepted(event); success(event, 1); service.reconcile(event.deliveryId());
        var result = published.getFirst();
        db.updateItem(r -> r.tableName(tableForKey(LifecycleRepository.key(event.deliveryId(), "FINAL"))).key(LifecycleRepository.key(event.deliveryId(), "FINAL"))
                .updateExpression("SET publish_state = :pending").expressionAttributeValues(Map.of(":pending", AttributeValue.fromS("PENDING"))));
        assertThrows(IllegalStateException.class, () -> new DeliveryCompactor(db).compact(result));
        lifecycle.published(event.deliveryId(), LifecycleRepository.text(lifecycle.read(event.deliveryId(), "FINAL"), "result_event"), NOW);
        db.updateItem(r -> r.tableName(tableForKey(LifecycleRepository.key(event.deliveryId(), "ATTEMPT#" + id(event, 1)))).key(LifecycleRepository.key(event.deliveryId(), "ATTEMPT#" + id(event, 1)))
                .updateExpression("REMOVE receipt_tracking_version"));
        assertThrows(IllegalStateException.class, () -> new DeliveryCompactor(db).compact(result));
        assertTrue(lifecycle.read(event.deliveryId(), "META").containsKey("payload")); assertFalse(item(event, 1).isEmpty());
    }

    @Test void failedConditionalDeleteRollsBackEntireCompaction() {
        var event = event(false); accepted(event); success(event, 1); service.reconcile(event.deliveryId());
        String receipt = receipts.getFirst();
        db.updateItem(r -> r.tableName(tableForKey(ReceiptResultRepository.receiptKey(receipt))).key(ReceiptResultRepository.receiptKey(receipt))
                .updateExpression("SET delivery_id = :different").expressionAttributeValues(Map.of(":different", AttributeValue.fromS("different-test-delivery"))));
        assertThrows(TransactionCanceledException.class, () -> new DeliveryCompactor(db).compact(published.getFirst()));
        assertTrue(lifecycle.read(event.deliveryId(), "META").containsKey("payload"));
        assertFalse(item(event, 1).isEmpty()); assertFalse(lifecycle.read(event.deliveryId(), "FINAL").isEmpty());
    }

    @Test void lostCompactionResponseIsResolvedByStableSmallCompletionRecord() {
        var event = event(false); accepted(event); success(event, 1); service.reconcile(event.deliveryId());
        var uncertain = mock(DynamoDbClient.class, org.mockito.AdditionalAnswers.delegatesTo(db));
        doAnswer(call -> {
            db.transactWriteItems((TransactWriteItemsRequest) call.getArgument(0));
            throw SdkClientException.create("lost cleanup response after commit");
        }).when(uncertain).transactWriteItems(any(TransactWriteItemsRequest.class));
        assertThrows(SdkClientException.class, () -> new DeliveryCompactor(uncertain).compact(published.getFirst()));
        assertFalse(new DeliveryCompactor(db).compact(published.getFirst()));
        assertTrue(DeliveryCompletion.compacted(lifecycle.read(event.deliveryId(), "META")));
        lifecycle.published(event.deliveryId(), mapper.writeValueAsString(published.getFirst()), NOW); // stale publisher acknowledgement
    }

    @Test void claimPausedBeforeTransactionCannotRecreateDeletedAttempt() throws Exception {
        var event = event(false); accepted(event); success(event, 1); service.reconcile(event.deliveryId());
        var reached = new CountDownLatch(1); var resume = new CountDownLatch(1);
        var paused = mock(DynamoDbClient.class, org.mockito.AdditionalAnswers.delegatesTo(db));
        doAnswer(call -> {
            reached.countDown(); assertTrue(resume.await(10, TimeUnit.SECONDS));
            return db.transactWriteItems((TransactWriteItemsRequest) call.getArgument(0));
        }).when(paused).transactWriteItems(any(TransactWriteItemsRequest.class));
        try (var thread = Executors.newSingleThreadExecutor()) {
            var claim = thread.submit(() -> new DispatchAttemptRepository(paused).claim(event, id(event, 1), PRIMARY, NOW, NOW.plusSeconds(1), NOW.plusSeconds(3)));
            try { assertTrue(reached.await(10, TimeUnit.SECONDS)); new DeliveryCompactor(db).compact(published.getFirst()); }
            finally { resume.countDown(); }
            assertEquals(DispatchClaimStatus.ALREADY_ACCEPTED, claim.get(10, TimeUnit.SECONDS).status());
            assertTrue(item(event, 1).isEmpty());
        }
    }

    @Test void mismatchingSqlResultCannotCompactOrAcknowledgeExistingCompletion() {
        var event = event(false); accepted(event); success(event, 1); service.reconcile(event.deliveryId());
        var e = published.getFirst();
        var changed = new DeliveryFinalized(e.schemaVersion(), e.eventType(), e.eventId(), e.deliveryId(), e.tenantId(), e.deliveryType(), "FAILED", "CHANGED", e.routeOrder(), e.attemptId(), e.provider(), e.occurredAt(), e.resultAt(), e.finalizedAt(), e.deadline());
        assertThrows(IllegalStateException.class, () -> new DeliveryCompactor(db).compact(changed));
        assertTrue(lifecycle.read(event.deliveryId(), "META").containsKey("payload"));
        new DeliveryCompactor(db).compact(e);
        assertThrows(IllegalStateException.class, () -> new DeliveryCompactor(db).compact(changed));
    }

    @Test void lostInitialTransactionalClaimDoesNotPermitBlindResend() {
        var event = event(false);
        var lost = mock(DynamoDbClient.class, org.mockito.AdditionalAnswers.delegatesTo(db));
        doAnswer(call -> {
            db.transactWriteItems((TransactWriteItemsRequest) call.getArgument(0));
            throw SdkClientException.create("initial claim commit response lost");
        }).when(lost).transactWriteItems(any(TransactWriteItemsRequest.class));
        assertThrows(SdkClientException.class, () -> new DispatchAttemptRepository(lost).claim(event, id(event, 1), PRIMARY, NOW, NOW.plusSeconds(1), NOW.plusSeconds(3)));
        assertEquals(DispatchClaimStatus.IN_PROGRESS, attempts.claim(event, id(event, 1), PRIMARY, NOW, NOW.plusSeconds(1), NOW.plusSeconds(3)).status());
        assertEquals(DispatchClaimStatus.REVIEW_REQUIRED, attempts.claim(event, id(event, 1), PRIMARY, NOW.plusSeconds(1), NOW.plusSeconds(2), NOW.plusSeconds(3)).status());
        assertEquals(0, primaryCalls.get());
    }

    LifecycleService service(LifecycleRepository repository, FinalizedPublisher publisher) {
        return new LifecycleService(repository, sources, dispatch, secondary, config, publisher, clock, metrics);
    }
    DeliveryEvent modernEvent(boolean fallback) {
        var event = event(fallback).forAdmission();
        db.updateItem(r -> r.tableName(ORIGIN).key(DeliveryCompletion.metaKey(event.requestKey()))
                .updateExpression("SET schema_version=:two,request_key=:key")
                .expressionAttributeValues(Map.of(":two", AttributeValue.fromN("2"), ":key", AttributeValue.fromS(event.requestKey()))));
        return event;
    }
    ManualResolution.Target manualTarget(DeliveryEvent event, int route, ManualResolution.Decision decision) {
        return new ManualResolution.Target(999, UUID.fromString(event.requestKey()), UUID.fromString(event.deliveryId()), UUID.fromString(id(event, route)),
                Long.parseLong(item(event, route).get("version").n()), UUID.randomUUID(), decision);
    }

    @Test void operatorSuccessUsesExistingFinalizationWithoutResendingAndFencesLaterReceipt() {
        var event = modernEvent(false); var claim = claim(event); clock.now = NOW.plusSeconds(2);
        var target = manualTarget(event, 1, ManualResolution.Decision.SUCCEEDED);
        assertEquals(ManualResolution.Outcome.APPLIED, new ManualResolution(db).apply(target, clock.now));
        service.reconcile(event.deliveryId()); assertEquals("DELIVERED", published.getFirst().outcome());
        assertEquals(clock.now, published.getFirst().resultAt()); assertEquals(NOW.plusSeconds(3), published.getFirst().deadline());
        assertEquals(0, primaryCalls.get()); assertEquals(0, secondaryCalls.get());
        assertThrows(ConditionalCheckFailedException.class, () -> attempts.markAccepted(claim, clock.now, clock.now));
        var before = item(event, 1); success(event, 1); assertEquals(before, item(event, 1));
        assertEquals(ManualResolution.Outcome.ALREADY_APPLIED, new ManualResolution(db).apply(target, NOW.plusSeconds(30)));
    }

    @Test void operatorPrimaryFailureStartsOnlyConfiguredFallbackFromDecisionTime() {
        var event = modernEvent(true); claim(event); clock.now = NOW.plusSeconds(2);
        assertEquals(ManualResolution.Outcome.APPLIED, new ManualResolution(db).apply(manualTarget(event, 1, ManualResolution.Decision.FAILED), clock.now));
        service.reconcile(event.deliveryId()); assertEquals(1, secondaryCalls.get()); assertTrue(published.isEmpty());
        assertEquals(NOW.plusSeconds(7), LifecycleRepository.deadline(item(event, 2)));
        db.updateItem(r -> r.tableName(STEP).key(LifecycleRepository.key(event.deliveryId(), "ATTEMPT#" + id(event, 2)))
                .updateExpression("SET #state=:review").expressionAttributeNames(Map.of("#state", "status"))
                .expressionAttributeValues(Map.of(":review", AttributeValue.fromS("REVIEW_REQUIRED"))));
        clock.now = NOW.plusSeconds(4);
        assertEquals(ManualResolution.Outcome.APPLIED, new ManualResolution(db).apply(manualTarget(event, 2, ManualResolution.Decision.FAILED), clock.now));
        service.reconcile(event.deliveryId()); assertEquals("FAILED", published.getFirst().outcome());
        assertEquals("OPERATOR_CONFIRMED_FAILED", published.getFirst().reason()); assertEquals(2, published.getFirst().routeOrder());
        assertEquals(NOW.plusSeconds(7), published.getFirst().deadline()); assertEquals(1, secondaryCalls.get());
    }

    @Test void operatorFailureCannotEnableFallbackAbsentFromOriginalRequest() {
        var event = modernEvent(false); claim(event); clock.now = NOW.plusSeconds(2);
        assertEquals(ManualResolution.Outcome.APPLIED, new ManualResolution(db).apply(manualTarget(event, 1, ManualResolution.Decision.FAILED), clock.now));
        service.reconcile(event.deliveryId()); assertEquals("FAILED", published.getFirst().outcome());
        assertEquals(0, secondaryCalls.get()); assertTrue(item(event, 2).isEmpty());
    }

    @Test void automaticExpiryWinsAgainstOperatorPausedBeforeConditionalCommit() {
        var event = modernEvent(false); claim(event); clock.now = NOW.plusSeconds(2);
        var paused = mock(DynamoDbClient.class, org.mockito.AdditionalAnswers.delegatesTo(db));
        doAnswer(call -> {
            assertTrue(lifecycle.expire(event.deliveryId(), item(event, 1), NOW.plusSeconds(3)));
            return db.transactWriteItems((TransactWriteItemsRequest) call.getArgument(0));
        }).when(paused).transactWriteItems(any(TransactWriteItemsRequest.class));
        assertEquals(ManualResolution.Outcome.STALE_OR_INELIGIBLE, new ManualResolution(paused).apply(manualTarget(event, 1, ManualResolution.Decision.SUCCEEDED), clock.now));
        clock.now = NOW.plusSeconds(3); service.reconcile(event.deliveryId());
        assertEquals("EXPIRED", published.getFirst().outcome()); assertEquals(NOW.plusSeconds(3), published.getFirst().resultAt());
        assertFalse(item(event, 1).containsKey("operator_action_id"));
    }

    @Test void actualReceiptWinsAgainstOperatorPausedBeforeConditionalCommit() {
        var event = modernEvent(false); claim(event); clock.now = NOW.plusSeconds(2);
        var paused = mock(DynamoDbClient.class, org.mockito.AdditionalAnswers.delegatesTo(db));
        doAnswer(call -> { success(event, 1); return db.transactWriteItems((TransactWriteItemsRequest) call.getArgument(0)); })
                .when(paused).transactWriteItems(any(TransactWriteItemsRequest.class));
        assertEquals(ManualResolution.Outcome.STALE_OR_INELIGIBLE, new ManualResolution(paused).apply(manualTarget(event, 1, ManualResolution.Decision.FAILED), clock.now));
        service.reconcile(event.deliveryId()); assertEquals("DELIVERED", published.getFirst().outcome());
        assertFalse(item(event, 1).containsKey("operator_action_id")); assertEquals(0, secondaryCalls.get());
    }

    DeliveryEvent event(boolean fallback) {
        var event = DeliveryEvent.requested(UUID.randomUUID().toString(), 999L, "SMS", Map.of("message", "test"), NOW, fallback).toDispatchRequested();
        events.add(event);
        var item = new HashMap<>(LifecycleRepository.key(event.deliveryId(), "META"));
        item.put("delivery_id", AttributeValue.fromS(event.deliveryId()));
        item.put("event_id", AttributeValue.fromS(DeliveryIds.eventId(event.deliveryId(), DeliveryEventType.DELIVERY_REQUESTED)));
        item.put("status", AttributeValue.fromS("ACCEPTED"));
        item.put("tenant_id", AttributeValue.fromN("999")); item.put("delivery_type", AttributeValue.fromS("SMS"));
        item.put("payload", AttributeValue.fromS(mapper.writeValueAsString(event.payload())));
        item.put("occurred_at", AttributeValue.fromS(NOW.toString())); item.put("fallback_allowed", AttributeValue.fromBool(fallback));
        LifecycleIndex.add(item, event.deliveryId(), 0);
        db.putItem(r -> r.tableName(tableForKey(item)).item(item)); return event;
    }
    DispatchAttempt claim(DeliveryEvent event) { return attempts.claim(event, id(event, 1), PRIMARY, NOW, NOW.plusSeconds(1), NOW.plusSeconds(3)).attempt(); }
    void accepted(DeliveryEvent event) { attempts.markAccepted(claim(event), NOW, NOW); }
    void success(DeliveryEvent event, int route) {
        var receipt = ReceiptEvent.received(UUID.randomUUID().toString(), event.deliveryId(), id(event, route), route == 1 ? PRIMARY : SECONDARY,
                route, ReceiptOutcome.DELIVERED, "DELIVERED", clock.instant(), clock.instant(), 1);
        receipts.add(receipt.eventId()); sources.apply(receipt, clock.instant());
    }
    static String id(DeliveryEvent event, int route) { return DeliveryIds.attemptId(event.deliveryId(), route == 1 ? PRIMARY : SECONDARY, route, 1); }
    Map<String, AttributeValue> item(DeliveryEvent event, int route) { return lifecycle.read(event.deliveryId(), "ATTEMPT#" + id(event, route)); }
    String state(DeliveryEvent event, int route) { return LifecycleRepository.text(item(event, route), "status"); }
    static class MutableClock extends Clock {
        Instant now = NOW;
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
        public Instant instant() { return now; }
    }
}

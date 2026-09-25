package event.delivery.dispatch.repository;

import event.common.delivery.DeliveryEvent;
import event.common.lifecycle.DeliveryCompletion;
import event.common.tcp.TcpFrames;
import event.common.tcp.TcpDeliveryRequest;
import event.common.tcp.TcpDeliveryResponse;
import event.common.delivery.DeliveryIds;
import event.common.dynamodb.config.DynamoDbTableInitializer;
import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import event.delivery.dispatch.model.DispatchAttempt;
import event.delivery.dispatch.model.DispatchClaim;
import event.delivery.dispatch.model.DispatchFailureDecision;
import event.delivery.dispatch.model.DispatchClaimStatus;
import event.delivery.dispatch.port.DeliveryProviderClient;
import event.delivery.dispatch.port.DispatchAttemptStore;
import event.delivery.dispatch.service.DispatchService;
import event.delivery.dispatch.service.SecondaryDispatchService;
import event.delivery.dispatch.model.SecondaryRoute;
import event.delivery.dispatch.config.SecondaryDispatchProperties;
import event.delivery.dispatch.port.SecondaryProviderClient;
import event.delivery.dispatch.service.DispatchRetryPendingException;
import event.delivery.dispatch.external.client.ProviderFailureException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import event.delivery.dispatch.config.DispatchFailureConfiguration;
import event.delivery.dispatch.consumer.DispatchRequestConsumer;
import tools.jackson.databind.json.JsonMapper;

import static event.common.dynamodb.DynamoDbAttributeNames.*;
import static event.common.dynamodb.DynamoDbTableNames.DELIVERY_STATE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@EnabledIfEnvironmentVariable(named = "DYNAMODB_TEST_ENDPOINT", matches = ".+")
class DispatchAttemptDynamoDbTest {

    private static final Instant NOW = Instant.parse("2026-09-24T00:00:00Z");
    private static final Instant LEASE_END = NOW.plusSeconds(30);
    private static final String PROVIDER = "test-provider-without-deduplication";
    private static DynamoDbClient client;
    private DispatchAttemptRepository repository;
    private DeliveryEvent event;
    private final List<DeliveryEvent> ownEvents = new ArrayList<>();
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger secondaryCalls = new AtomicInteger();
    private SecondaryProviderClient secondaryClient = (request, key) -> {
        secondaryCalls.incrementAndGet();
        return new ProviderDispatchResponse(request.deliveryId(), true, NOW);
    };
    private final DeliveryProviderClient provider = (request, idempotencyKey) -> {
        calls.incrementAndGet(); // Every invocation has an effect; no provider deduplication.
        return new ProviderDispatchResponse(request.deliveryId(), true, NOW.plusSeconds(1));
    };

    @BeforeAll
    static void connect() {
        var endpoint = URI.create(System.getenv("DYNAMODB_TEST_ENDPOINT"));
        if (!"http".equals(endpoint.getScheme())
                || !("localhost".equals(endpoint.getHost()) || "127.0.0.1".equals(endpoint.getHost()))) {
            throw new IllegalArgumentException("DYNAMODB_TEST_ENDPOINT must point to local HTTP DynamoDB");
        }
        client = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .overrideConfiguration(config -> config.apiCallTimeout(Duration.ofSeconds(10))).build();
        new DynamoDbTableInitializer(client).run(null);
    }

    @BeforeEach
    void setUp() {
        repository = new DispatchAttemptRepository(client);
        event = newEvent();
    }

    @AfterEach
    void cleanUp() {
        for (var ownEvent : ownEvents) {
            var parent = stored(ownEvent);
            if (parent.containsKey("secondary_attempt_id")) {
                client.deleteItem(request -> request.tableName(DELIVERY_STATE).key(Map.of(
                        PK, AttributeValue.fromS("DELIVERY#" + ownEvent.deliveryId()),
                        SK, AttributeValue.fromS("ATTEMPT#" + parent.get("secondary_attempt_id").s()))));
            }
            client.deleteItem(request -> request.tableName(DELIVERY_STATE).key(key(ownEvent)));
            client.deleteItem(request -> request.tableName(DELIVERY_STATE).key(DeliveryCompletion.metaKey(ownEvent.deliveryId())));
        }
    }

    @AfterAll
    static void close() {
        if (client != null) client.close();
    }

    @Test
    void expiredLeaseAfterPreCallCrashIsDurablyHeldForReview() {
        claim(event, NOW); // The process stopped before the external call.

        service(repository, LEASE_END).dispatch(event);
        service(repository, LEASE_END.plusSeconds(3600)).dispatch(event);

        assertEquals(0, calls.get());
        assertReview(event);
        assertEquals("2", stored(event).get(VERSION).n());
        assertEquals(Long.toString(LEASE_END.toEpochMilli()), stored(event).get(LEASE_UNTIL).n());
    }

    @Test
    void providerEffectWithoutResultRecordIsNotRepeatedOnReplay() {
        DispatchAttemptStore failingResultStore = new DispatchAttemptStore() {
            @Override
            public DispatchClaim claim(DeliveryEvent request, String attemptId, String provider, Instant now, Instant until, Instant deadline) {
                return repository.claim(request, attemptId, provider, now, until, deadline);
            }

            @Override
            public java.util.Optional<SecondaryRoute> prepareSecondary(DeliveryEvent event, String id, String provider, Duration ttl) {
                return repository.prepareSecondary(event, id, provider, ttl);
            }

            @Override
            public DispatchClaim claimSecondary(DeliveryEvent event, SecondaryRoute route, Instant now, Instant until) {
                return repository.claimSecondary(event, route, now, until);
            }

            @Override
            public void recordFailure(DispatchAttempt attempt, DispatchFailureDecision decision) {
                repository.recordFailure(attempt, decision);
            }

            @Override
            public void markAccepted(DispatchAttempt attempt, Instant processedAt, Instant now) {
                throw new IllegalStateException("process stopped before result storage");
            }
        };
        assertThrows(IllegalStateException.class, () -> service(failingResultStore, NOW).dispatch(event));

        service(repository, LEASE_END).dispatch(event);
        service(repository, LEASE_END.plusSeconds(60)).dispatch(event);

        assertEquals(1, calls.get());
        assertReview(event);
    }

    @Test
    void recordedAcceptanceStillSkipsProviderAfterLeaseTime() {
        service(repository, NOW).dispatch(event);

        service(repository, LEASE_END.plusSeconds(86400)).dispatch(event);

        assertEquals(1, calls.get());
        assertEquals("ACCEPTED", stored(event).get(STATUS).s());
        assertFalse(stored(event).containsKey(LEASE_UNTIL));
    }

    @Test
    void activeLeaseIsNotRenewedOrTakenOverByDuplicate() {
        assertEquals(DispatchClaimStatus.CLAIMED, claim(event, NOW).status());

        assertEquals(DispatchClaimStatus.IN_PROGRESS, claim(event, NOW.plusSeconds(29)).status());

        assertEquals("PROCESSING", stored(event).get(STATUS).s());
        assertEquals("1", stored(event).get(VERSION).n());
        assertEquals(Long.toString(LEASE_END.toEpochMilli()), stored(event).get(LEASE_UNTIL).n());
    }

    @Test
    void concurrentRecoveryCreatesOneReviewTransitionWithoutReclaim() throws Exception {
        claim(event, NOW);
        var start = new CountDownLatch(1);
        var results = new ArrayList<Future<DispatchClaim>>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 20; i++) {
                results.add(executor.submit(() -> {
                    start.await();
                    return claim(event, LEASE_END);
                }));
            }
            start.countDown();
            for (var result : results) {
                assertEquals(DispatchClaimStatus.REVIEW_REQUIRED, result.get(15, TimeUnit.SECONDS).status());
            }
        }
        assertReview(event);
        assertEquals("2", stored(event).get(VERSION).n());
    }

    @Test
    void oldOwnerCannotOverwriteReviewDecision() {
        var original = claim(event, NOW).attempt();
        claim(event, LEASE_END);

        assertThrows(ConditionalCheckFailedException.class,
                () -> repository.markAccepted(original, LEASE_END, LEASE_END));

        assertReview(event);
    }

    @Test
    void resultAndReviewRaceHasExactlyOneWinningState() throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 10; i++) {
                var racingEvent = newEvent();
                var original = claim(racingEvent, NOW).attempt();
                var start = new CountDownLatch(1);
                var completion = executor.submit(() -> {
                    start.await();
                    try {
                        repository.markAccepted(original, LEASE_END, LEASE_END);
                        return true;
                    } catch (ConditionalCheckFailedException fenced) {
                        return false;
                    }
                });
                var recovery = executor.submit(() -> {
                    start.await();
                    return claim(racingEvent, LEASE_END);
                });
                start.countDown();
                boolean accepted = completion.get(15, TimeUnit.SECONDS);
                assertEquals(accepted ? DispatchClaimStatus.ALREADY_ACCEPTED : DispatchClaimStatus.REVIEW_REQUIRED,
                        recovery.get(15, TimeUnit.SECONDS).status());
                assertEquals(accepted ? "ACCEPTED" : "REVIEW_REQUIRED", stored(racingEvent).get(STATUS).s());
            }
        }
    }

    @Test
    void lostReviewWriteResponseCanBeResolvedWithoutAnotherProviderCall() {
        claim(event, NOW);
        var responseLossClient = mock(DynamoDbClient.class, delegatesTo(client));
        doAnswer(invocation -> {
            UpdateItemRequest request = invocation.getArgument(0);
            var result = client.updateItem(request);
            if (request.expressionAttributeValues().containsKey(":review")) {
                throw SdkClientException.create("response lost after durable review write");
            }
            return result;
        }).when(responseLossClient).updateItem(any(UpdateItemRequest.class));
        var responseLossRepository = new DispatchAttemptRepository(responseLossClient);

        assertThrows(SdkClientException.class, () -> service(responseLossRepository, LEASE_END).dispatch(event));
        service(repository, LEASE_END).dispatch(event);

        assertEquals(0, calls.get());
        assertReview(event);
    }

    @Test
    void restartReadsReservationAndOnlyDueWorkerCanClaim() throws Exception {
        var first = claim(event, NOW).attempt();
        repository.recordFailure(first, retryAt(NOW.plusSeconds(10)));
        repository = new DispatchAttemptRepository(client);
        assertEquals(DispatchClaimStatus.RETRY_WAIT, claim(event, NOW.plusSeconds(9)).status());
        var start = new CountDownLatch(1);
        var results = new ArrayList<Future<DispatchClaim>>();
        DispatchAttempt winner = null;
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 20; i++) results.add(executor.submit(() -> {
                start.await();
                return claim(event, NOW.plusSeconds(10));
            }));
            start.countDown();
            int claimed = 0;
            for (var result : results) {
                var value = result.get(15, TimeUnit.SECONDS);
                if (value.status() == DispatchClaimStatus.CLAIMED) {
                    claimed++;
                    winner = value.attempt();
                } else assertEquals(DispatchClaimStatus.IN_PROGRESS, value.status());
            }
            assertEquals(1, claimed);
        }
        assertNotNull(winner);
        assertEquals(1, winner.retryCount());
        assertEquals(first.attemptId(), winner.attemptId());
        assertEquals(2, winner.version());
        assertThrows(ConditionalCheckFailedException.class, () -> repository.markAccepted(first, NOW, NOW));
        assertThrows(ConditionalCheckFailedException.class, () -> repository.recordFailure(first, retryAt(NOW.plusSeconds(20))));
        repository.markAccepted(winner, NOW.plusSeconds(10), NOW.plusSeconds(10));
        assertEquals(DispatchClaimStatus.ALREADY_ACCEPTED, claim(event, NOW.plusSeconds(11)).status());
    }

    @ParameterizedTest
    @EnumSource(value = ProviderFailureException.Kind.class, names = {"RETRY_1S", "RETRY_10S", "NO_RESPONSE"})
    void exactlyThreeRetriesSurviveNewServiceInstances(ProviderFailureException.Kind kind) {
        var callCount = new AtomicInteger();
        DeliveryProviderClient failingProvider = (request, key) -> {
            callCount.incrementAndGet();
            assertEquals(attemptId(request), key);
            throw new ProviderFailureException(kind);
        };
        for (int retry = 0; retry <= 3; retry++) {
            var current = service(new DispatchAttemptRepository(client), failingProvider, NOW.plusSeconds(retry * 10L));
            if (retry < 3) assertThrows(DispatchRetryPendingException.class, () -> current.dispatch(event));
            else current.dispatch(event);
        }
        service(repository, failingProvider, NOW.plusSeconds(100)).dispatch(event);
        assertEquals(4, callCount.get());
        assertEquals("3", stored(event).get(RETRY_COUNT).n());
        assertEquals("DECISION_PENDING", stored(event).get(STATUS).s());
        assertEquals("RETRY_EXHAUSTED_" + kind, stored(event).get(FAILURE_REASON).s());
        assertFalse(stored(event).containsKey(NEXT_ATTEMPT_AT));
    }

    @Test
    void lostFailureWriteAcknowledgementDoesNotLoseOrDuplicateReservation() {
        var lossClient = mock(DynamoDbClient.class, delegatesTo(client));
        doAnswer(invocation -> {
            UpdateItemRequest request = invocation.getArgument(0);
            var result = client.updateItem(request);
            if (request.expressionAttributeValues().containsKey(":state")) {
                throw SdkClientException.create("response lost after failure write");
            }
            return result;
        }).when(lossClient).updateItem(any(UpdateItemRequest.class));
        DeliveryProviderClient failing = (request, key) -> {
            calls.incrementAndGet();
            throw new ProviderFailureException(ProviderFailureException.Kind.RETRY_10S);
        };
        assertThrows(SdkClientException.class, () -> service(new DispatchAttemptRepository(lossClient), failing, NOW).dispatch(event));
        assertThrows(DispatchRetryPendingException.class, () -> service(repository, NOW.plusSeconds(9)).dispatch(event));
        assertEquals(1, calls.get());
        service(repository, NOW.plusSeconds(10)).dispatch(event);
        service(repository, NOW.plusSeconds(11)).dispatch(event);
        assertEquals(2, calls.get());
        assertEquals("ACCEPTED", stored(event).get(STATUS).s());
    }

    @Test
    void failureBeforeDurableRecordingRequiresReviewWithoutAutomaticRetry() {
        var lossClient = mock(DynamoDbClient.class, delegatesTo(client));
        doAnswer(invocation -> {
            UpdateItemRequest request = invocation.getArgument(0);
            if (request.expressionAttributeValues().containsKey(":state")) throw SdkClientException.create("storage unavailable");
            return client.updateItem(request);
        }).when(lossClient).updateItem(any(UpdateItemRequest.class));
        DeliveryProviderClient failing = (request, key) -> {
            calls.incrementAndGet();
            throw new ProviderFailureException(ProviderFailureException.Kind.NO_RESPONSE);
        };
        assertThrows(SdkClientException.class, () -> service(new DispatchAttemptRepository(lossClient), failing, NOW).dispatch(event));
        service(repository, LEASE_END).dispatch(event);
        assertEquals(1, calls.get());
        assertReview(event);
    }

    @Test
    void resumedRetryWithoutResultDoesNotBecomeAnotherScheduledRetry() {
        var first = claim(event, NOW).attempt();
        repository.recordFailure(first, retryAt(NOW.plusSeconds(1)));
        assertEquals(DispatchClaimStatus.CLAIMED, claim(event, NOW.plusSeconds(1)).status());
        service(repository, NOW.plusSeconds(31)).dispatch(event);
        assertReview(event);
        assertEquals(0, calls.get());
        assertEquals("1", stored(event).get(RETRY_COUNT).n());
    }

    @Test
    void retryDeadlineIsPersistedAndCannotBeExtendedByConfigurationChange() {
        var first = repository.claim(event, attemptId(event), PROVIDER, NOW, LEASE_END, NOW.plusSeconds(5)).attempt();
        repository.recordFailure(first, retryAt(NOW.plusSeconds(10)));
        assertEquals(DispatchClaimStatus.RETRY_WAIT, claim(event, NOW.plusSeconds(4)).status());
        assertEquals(DispatchClaimStatus.DECISION_PENDING, claim(event, NOW.plusSeconds(5)).status());
        assertEquals("PRIMARY_EXPIRED", stored(event).get(FAILURE_REASON).s());
        assertEquals(NOW.plusSeconds(5).toString(), stored(event).get(FAILURE_OBSERVED_AT).s());
        assertEquals("0", stored(event).get(RETRY_COUNT).n());
    }

    @Test
    void lateAcceptanceIsStoredAsExpiryWithoutASecondProviderCall() {
        var time = new java.util.concurrent.atomic.AtomicReference<>(NOW);
        Clock clock = new Clock() {
            public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
            public Clock withZone(java.time.ZoneId zone) { return this; }
            public Instant instant() { return time.get(); }
        };
        DeliveryProviderClient late = (request, key) -> {
            calls.incrementAndGet();
            time.set(NOW.plusSeconds(10800));
            return new ProviderDispatchResponse(request.deliveryId(), true, time.get());
        };
        new DispatchService(repository, late, new DispatchProperties(PROVIDER, Duration.ofSeconds(30)), clock, metrics(), secondary(repository, clock)).dispatch(event);
        service(repository, NOW.plusSeconds(10801)).dispatch(event);
        assertEquals(1, calls.get());
        assertEquals("PRIMARY_EXPIRED", stored(event).get(FAILURE_REASON).s());
        assertEquals("DECISION_PENDING", stored(event).get(STATUS).s());
    }

    private DispatchFailureDecision retryAt(Instant due) {
        return new DispatchFailureDecision(DispatchFailureDecision.State.RETRY_SCHEDULED, "RETRY_10S", NOW, due);
    }

    @Test
    void primaryFailureHandsOffToSecondaryOnceAndKeepsDecisionBasedDeadline() {
        allowFallback();
        service(repository, fallbackProvider(), NOW).dispatch(event);
        service(new DispatchAttemptRepository(client), fallbackProvider(), NOW.plusSeconds(100)).dispatch(event);
        assertEquals(1, calls.get());
        assertEquals(1, secondaryCalls.get());
        assertEquals("ACCEPTED", storedSecondary().get(STATUS).s());
        assertEquals(Long.toString(NOW.plusSeconds(14400).toEpochMilli()), storedSecondary().get(DEADLINE_AT).n());
        assertEquals("2", storedSecondary().get(ROUTE_ORDER).n());
    }

    @Test
    void defaultDisallowedAndPermanentRejectionDoNotCallSecondary() {
        service(repository, fallbackProvider(), NOW).dispatch(event);
        assertEquals(0, secondaryCalls.get());
        assertFalse(stored(event).containsKey("secondary_attempt_id"));
        event = newEvent();
        allowFallback();
        service(repository, (request, key) -> {
            throw new ProviderFailureException(ProviderFailureException.Kind.PERMANENT_REJECTION);
        }, NOW).dispatch(event);
        assertEquals(0, secondaryCalls.get());
        assertFalse(stored(event).containsKey("secondary_attempt_id"));
    }

    @Test
    void exhaustedNoResponseImmediatelyStartsSecondaryWithoutWaitingForPrimaryExpiry() {
        allowFallback();
        DeliveryProviderClient noResponse = (request, key) -> {
            calls.incrementAndGet();
            throw new ProviderFailureException(ProviderFailureException.Kind.NO_RESPONSE);
        };
        for (int i = 0; i < 3; i++) {
            var worker = service(repository, noResponse, NOW.plusSeconds(i));
            assertThrows(DispatchRetryPendingException.class, () -> worker.dispatch(event));
        }
        service(repository, noResponse, NOW.plusSeconds(3)).dispatch(event);
        assertEquals(4, calls.get());
        assertEquals(1, secondaryCalls.get());
        assertEquals(Long.toString(NOW.plusSeconds(14403).toEpochMilli()), storedSecondary().get(DEADLINE_AT).n());
    }

    @Test
    void primaryExpiryAnchorsSecondaryAtPrimaryDeadlineAndOverdueSecondaryNeverSends() {
        allowFallback();
        service(repository, NOW.plusSeconds(10810)).dispatch(event);
        assertEquals(0, calls.get());
        assertEquals(1, secondaryCalls.get());
        assertEquals(Long.toString(NOW.plusSeconds(25200).toEpochMilli()), storedSecondary().get(DEADLINE_AT).n());
        event = newEvent();
        allowFallback();
        service(repository, NOW.plusSeconds(25200)).dispatch(event);
        assertEquals(1, secondaryCalls.get());
        assertEquals("SECONDARY_EXPIRED", storedSecondary().get(FAILURE_REASON).s());
    }

    @Test
    void concurrentFallbackReplaysCreateOneSecondaryEffect() throws Exception {
        allowFallback();
        var start = new CountDownLatch(1);
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new ArrayList<Future<?>>();
            for (int i = 0; i < 20; i++) tasks.add(pool.submit(() -> {
                start.await();
                try { service(repository, fallbackProvider(), NOW).dispatch(event); }
                catch (event.delivery.dispatch.service.DispatchAttemptInProgressException expected) { }
                return null;
            }));
            start.countDown();
            for (var task : tasks) task.get(15, TimeUnit.SECONDS);
        }
        service(repository, fallbackProvider(), NOW).dispatch(event);
        assertEquals(1, calls.get());
        assertEquals(1, secondaryCalls.get());
    }

    @Test
    void lostSecondaryBindingResponseIsRecoveredWithoutChangingProviderOrDeadline() {
        allowFallback();
        var lost = mock(DynamoDbClient.class, delegatesTo(client));
        doAnswer(invocation -> {
            UpdateItemRequest request = invocation.getArgument(0);
            var result = client.updateItem(request);
            if (request.expressionAttributeValues().containsKey(":secondary")) throw SdkClientException.create("binding response lost");
            return result;
        }).when(lost).updateItem(any(UpdateItemRequest.class));
        assertThrows(SdkClientException.class, () -> service(new DispatchAttemptRepository(lost), fallbackProvider(), NOW).dispatch(event));
        assertEquals(0, secondaryCalls.get());
        var bound = repository.prepareSecondary(event, attemptId(event), "changed-provider", Duration.ofHours(99)).orElseThrow();
        assertEquals("tcp-provider", bound.provider());
        assertEquals(NOW.plusSeconds(14400), bound.deadline());
        var changedConfiguration = new SecondaryDispatchService(repository, secondaryClient,
                new SecondaryDispatchProperties("changed-provider", Duration.ofHours(99)),
                new DispatchProperties(PROVIDER, Duration.ofSeconds(30)), Clock.fixed(NOW, ZoneOffset.UTC), metrics());
        assertThrows(IllegalStateException.class, () -> changedConfiguration.dispatch(event, attemptId(event)));
        assertEquals(0, secondaryCalls.get());
        service(repository, fallbackProvider(), NOW.plusSeconds(10)).dispatch(event);
        assertEquals(1, calls.get());
        assertEquals(1, secondaryCalls.get());
    }

    @Test
    void secondaryEffectWithoutResultStorageRequiresReviewNotAnotherTcpCall() {
        allowFallback();
        var lost = mock(DynamoDbClient.class, delegatesTo(client));
        doAnswer(invocation -> {
            UpdateItemRequest request = invocation.getArgument(0);
            if (request.expressionAttributeValues().containsKey(":accepted")) throw SdkClientException.create("result write failed");
            return client.updateItem(request);
        }).when(lost).updateItem(any(UpdateItemRequest.class));
        assertThrows(SdkClientException.class, () -> service(new DispatchAttemptRepository(lost), fallbackProvider(), NOW).dispatch(event));
        service(repository, fallbackProvider(), NOW.plusSeconds(31)).dispatch(event);
        assertEquals(1, calls.get());
        assertEquals(1, secondaryCalls.get());
        assertEquals("REVIEW_REQUIRED", storedSecondary().get(STATUS).s());
    }

    @Test
    void secondaryNoResponseRetriesStayOnSecondaryRouteAndDeadline() {
        allowFallback();
        secondaryClient = (request, key) -> {
            secondaryCalls.incrementAndGet();
            throw new ProviderFailureException(ProviderFailureException.Kind.NO_RESPONSE);
        };
        for (int i = 0; i < 3; i++) {
            var worker = service(repository, fallbackProvider(), NOW.plusSeconds(i));
            assertThrows(DispatchRetryPendingException.class, () -> worker.dispatch(event));
        }
        service(repository, fallbackProvider(), NOW.plusSeconds(3)).dispatch(event);
        service(repository, fallbackProvider(), NOW.plusSeconds(4)).dispatch(event);
        assertEquals(1, calls.get());
        assertEquals(4, secondaryCalls.get());
        assertEquals("RETRY_EXHAUSTED_NO_RESPONSE", storedSecondary().get(FAILURE_REASON).s());
        assertEquals(Long.toString(NOW.plusSeconds(14400).toEpochMilli()), storedSecondary().get(DEADLINE_AT).n());
    }

    @Test
    void primaryHttpAndSecondaryTcpWorkTogetherWithRealSocketsAndDynamoDb() throws Exception {
        allowFallback();
        var http = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        var mapper = JsonMapper.builder().build();
        http.createContext("/api/v1/deliveries", exchange -> {
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                calls.incrementAndGet();
                byte[] reply = mapper.writeValueAsBytes(new ProviderDispatchResponse(event.deliveryId(), false, NOW, "FALLBACK"));
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(503, reply.length);
                exchange.getResponseBody().write(reply);
            }
        });
        http.start();
        try (var tcp = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress());
             var pool = Executors.newVirtualThreadPerTaskExecutor();
             var sender = new event.delivery.dispatch.external.client.TcpProviderClient(
                     new event.delivery.dispatch.external.config.TcpProviderProperties("localhost", tcp.getLocalPort(), Duration.ofSeconds(2), Duration.ofSeconds(2)), mapper)) {
            tcp.setSoTimeout(3000);
            var receiver = pool.submit(() -> {
                try (var socket = tcp.accept()) {
                    socket.setSoTimeout(3000);
                    var request = mapper.readValue(TcpFrames.read(socket.getInputStream()), TcpDeliveryRequest.class);
                    secondaryCalls.incrementAndGet();
                    assertEquals(event.deliveryId(), request.deliveryId());
                    TcpFrames.write(socket.getOutputStream(), mapper.writeValueAsBytes(
                            new TcpDeliveryResponse(request.deliveryId(), request.attemptId(), true, NOW, "RECEIVED")));
                }
                return null;
            });
            secondaryClient = sender;
            var httpClient = new event.delivery.dispatch.external.client.ExternalApiClient(
                    new event.delivery.dispatch.external.config.ExternalApiClientConfig().externalApiRestClient(
                            org.springframework.web.client.RestClient.builder(),
                            new event.delivery.dispatch.external.config.ExternalApiProperties("http://127.0.0.1:" + http.getAddress().getPort(), Duration.ofSeconds(2), Duration.ofSeconds(2))),
                    new DispatchProperties(PROVIDER, Duration.ofSeconds(30)));
            service(repository, httpClient, NOW).dispatch(event);
            service(repository, httpClient, NOW.plusSeconds(1)).dispatch(event);
            receiver.get(5, TimeUnit.SECONDS);
            assertEquals(1, calls.get());
            assertEquals(1, secondaryCalls.get());
            assertEquals("ACCEPTED", storedSecondary().get(STATUS).s());
        } finally { http.stop(0); }
    }

    private void allowFallback() {
        event = DeliveryEvent.requested(event.deliveryId(), event.tenantId(), event.deliveryType(), event.payload(), event.occurredAt(), true).toDispatchRequested();
    }

    @Test
    void legacyPrimaryDeadlineRemainsReadableAfterUpgrade() {
        var original = claim(event, NOW).attempt();
        repository.recordFailure(original, retryAt(NOW.plusSeconds(1)));
        client.updateItem(request -> request.tableName(DELIVERY_STATE).key(key(event))
                .updateExpression("SET primary_deadline = deadline_at REMOVE deadline_at, route_order"));
        var resumed = claim(event, NOW.plusSeconds(1));
        assertEquals(DispatchClaimStatus.CLAIMED, resumed.status());
        assertEquals(NOW.plusSeconds(10800), resumed.attempt().deadline());
        assertEquals(1, resumed.attempt().routeOrder());
    }

    private DeliveryProviderClient fallbackProvider() {
        return (request, key) -> {
            calls.incrementAndGet();
            throw new ProviderFailureException(ProviderFailureException.Kind.FALLBACK_REQUIRED);
        };
    }

    private Map<String, AttributeValue> storedSecondary() {
        var id = stored(event).get("secondary_attempt_id").s();
        return client.getItem(request -> request.tableName(DELIVERY_STATE).consistentRead(true).key(Map.of(
                PK, AttributeValue.fromS("DELIVERY#" + event.deliveryId()), SK, AttributeValue.fromS("ATTEMPT#" + id)))).item();
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "KAFKA_TEST_BOOTSTRAP_SERVERS", matches = ".+")
    void kafkaOffsetRemainsBeforeReservationAcrossConsumerRestartThenCommitsOnceAccepted() throws Exception {
        String bootstrap = System.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS");
        for (String address : bootstrap.split(",")) {
            if (!address.trim().matches("(localhost|127\\.0\\.0\\.1):[0-9]+")) {
                throw new IllegalArgumentException("Only local test Kafka endpoints are allowed");
            }
        }
        String topic = "test.dispatch-retry." + UUID.randomUUID();
        String group = topic + ".worker";
        var time = new java.util.concurrent.atomic.AtomicReference<>(NOW);
        Clock clock = new Clock() {
            public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
            public Clock withZone(java.time.ZoneId zone) { return this; }
            public Instant instant() { return time.get(); }
        };
        DeliveryProviderClient firstFails = (request, key) -> {
            if (request.deliveryId().equals(event.deliveryId()) && calls.incrementAndGet() == 1) {
                throw new ProviderFailureException(ProviderFailureException.Kind.RETRY_10S);
            }
            return new ProviderDispatchResponse(request.deliveryId(), true, time.get());
        };
        var observedPending = new java.util.concurrent.atomic.AtomicReference<>(new CountDownLatch(1));
        KafkaMessageListenerContainer<String, DeliveryEvent> container = null;
        try (var admin = Admin.create(Map.of("bootstrap.servers", bootstrap, "request.timeout.ms", 5000, "default.api.timeout.ms", 10000));
             var input = new KafkaProducer<byte[], byte[]>(Map.of("bootstrap.servers", bootstrap, "acks", "all"),
                     new ByteArraySerializer(), new ByteArraySerializer())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            try {
                container = retryContainer(bootstrap, topic, group, clock, firstFails, observedPending);
                container.start();
                var mapper = JsonMapper.builder().build();
                input.send(new ProducerRecord<>(topic, mapper.writeValueAsBytes(newEvent()))).get(10, TimeUnit.SECONDS);
                awaitCommit(admin, group, topic, 1);
                input.send(new ProducerRecord<>(topic, mapper.writeValueAsBytes(event))).get(10, TimeUnit.SECONDS);
                assertTrue(observedPending.get().await(10, TimeUnit.SECONDS));
                assertEquals("RETRY_SCHEDULED", stored(event).get(STATUS).s());
                container.stop();
                assertEquals(1, committed(admin, group, topic));

                observedPending.set(new CountDownLatch(1));
                container = retryContainer(bootstrap, topic, group, clock, firstFails, observedPending);
                container.start();
                assertTrue(observedPending.get().await(10, TimeUnit.SECONDS));
                assertEquals(1, calls.get(), "Restart before due time must not call provider");
                assertEquals(1, committed(admin, group, topic));
                time.set(NOW.plusSeconds(10));
                awaitCommit(admin, group, topic, 2);
                assertEquals("ACCEPTED", stored(event).get(STATUS).s());
                input.send(new ProducerRecord<>(topic, mapper.writeValueAsBytes(event))).get(10, TimeUnit.SECONDS);
                awaitCommit(admin, group, topic, 3);
                assertEquals(2, calls.get(), "Duplicate input after completion must not repeat the retry");
            } finally {
                if (container != null) container.stop();
                admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
                admin.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
            }
        }
    }

    private KafkaMessageListenerContainer<String, DeliveryEvent> retryContainer(
            String bootstrap, String topic, String group, Clock clock, DeliveryProviderClient sender,
            java.util.concurrent.atomic.AtomicReference<CountDownLatch> pending) {
        var consumerFactory = new DefaultKafkaConsumerFactory<>(Map.of("bootstrap.servers", bootstrap, "group.id", group,
                "enable.auto.commit", false, "auto.offset.reset", "earliest"), new StringDeserializer(),
                new JacksonJsonDeserializer<>(DeliveryEvent.class, false));
        var properties = new ContainerProperties(new TopicPartitionOffset(topic, 0));
        properties.setGroupId(group);
        properties.setAckMode(ContainerProperties.AckMode.RECORD);
        properties.setPollTimeout(100);
        properties.setShutdownTimeout(5000);
        var service = new DispatchService(new DispatchAttemptRepository(client), sender,
                new DispatchProperties(PROVIDER, Duration.ofSeconds(30)), clock, metrics(), secondary(new DispatchAttemptRepository(client), clock));
        var listener = new DispatchRequestConsumer(service);
        properties.setMessageListener((MessageListener<String, DeliveryEvent>) record -> {
            try {
                listener.consume(record.value());
            } catch (DispatchRetryPendingException expected) {
                pending.get().countDown();
                throw expected;
            }
        });
        var container = new KafkaMessageListenerContainer<>(consumerFactory, properties);
        container.setCommonErrorHandler(new DispatchFailureConfiguration().dispatchErrorHandler(50));
        return container;
    }

    private long committed(Admin admin, String group, String topic) throws Exception {
        var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        var offset = offsets.get(new TopicPartition(topic, 0));
        return offset == null ? -1 : offset.offset();
    }

    private void awaitCommit(Admin admin, String group, String topic, long expected) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            if (committed(admin, group, topic) == expected) return;
            Thread.sleep(20);
        }
        assertEquals(expected, committed(admin, group, topic));
    }

    private DispatchService service(DispatchAttemptStore store, DeliveryProviderClient sender, Instant now) {
        return new DispatchService(store, sender, new DispatchProperties(PROVIDER, Duration.ofSeconds(30)),
                Clock.fixed(now, ZoneOffset.UTC), metrics(), secondary(store, Clock.fixed(now, ZoneOffset.UTC)));
    }

    private DeliveryEvent newEvent() {
        var result = DeliveryEvent.requested(DeliveryIds.deliveryId(999L, "dispatch-test-" + UUID.randomUUID()),
                999L, "EMAIL", Map.of("body", "test"), NOW).toDispatchRequested();
        var origin = new java.util.HashMap<>(DeliveryCompletion.metaKey(result.deliveryId()));
        origin.put("delivery_id", AttributeValue.fromS(result.deliveryId()));
        client.putItem(r -> r.tableName(DELIVERY_STATE).item(origin));
        ownEvents.add(result);
        return result;
    }

    private DispatchClaim claim(DeliveryEvent request, Instant now) {
        return repository.claim(request, attemptId(request), PROVIDER, now, now.plusSeconds(30), NOW.plusSeconds(10800));
    }

    private DispatchService service(DispatchAttemptStore store, Instant now) {
        return new DispatchService(store, provider, new DispatchProperties(PROVIDER, Duration.ofSeconds(30)),
                Clock.fixed(now, ZoneOffset.UTC), metrics(), secondary(store, Clock.fixed(now, ZoneOffset.UTC)));
    }

    private SecondaryDispatchService secondary(DispatchAttemptStore store, Clock clock) {
        return new SecondaryDispatchService(store, secondaryClient, new SecondaryDispatchProperties(null, null),
                new DispatchProperties(PROVIDER, Duration.ofSeconds(30)), clock, metrics());
    }

    private String attemptId(DeliveryEvent request) {
        return DeliveryIds.attemptId(request.deliveryId(), PROVIDER, 1, 1);
    }

    private Map<String, AttributeValue> key(DeliveryEvent request) {
        return Map.of(PK, AttributeValue.fromS("DELIVERY#" + request.deliveryId()),
                SK, AttributeValue.fromS("ATTEMPT#" + attemptId(request)));
    }

    private Map<String, AttributeValue> stored(DeliveryEvent request) {
        return client.getItem(read -> read.tableName(DELIVERY_STATE).key(key(request)).consistentRead(true)).item();
    }

    private void assertReview(DeliveryEvent request) {
        var item = stored(request);
        assertEquals("REVIEW_REQUIRED", item.get(STATUS).s());
        assertEquals("LEASE_EXPIRED_WITHOUT_RESULT", item.get(REVIEW_REASON).s());
    }
    private static event.common.metrics.DeliveryMetrics metrics() {
        return new event.common.metrics.DeliveryMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
    }

}

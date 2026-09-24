package event.delivery.dispatch.repository;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryIds;
import event.common.dynamodb.config.DynamoDbTableInitializer;
import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import event.delivery.dispatch.model.DispatchAttempt;
import event.delivery.dispatch.model.DispatchClaim;
import event.delivery.dispatch.model.DispatchClaimStatus;
import event.delivery.dispatch.port.DeliveryProviderClient;
import event.delivery.dispatch.port.DispatchAttemptStore;
import event.delivery.dispatch.service.DispatchService;
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
            client.deleteItem(request -> request.tableName(DELIVERY_STATE).key(key(ownEvent)));
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
            public DispatchClaim claim(DeliveryEvent request, String attemptId, String provider, Instant now, Instant until) {
                return repository.claim(request, attemptId, provider, now, until);
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

    private DeliveryEvent newEvent() {
        var result = DeliveryEvent.requested(DeliveryIds.deliveryId(999L, "dispatch-test-" + UUID.randomUUID()),
                999L, "EMAIL", Map.of("body", "test"), NOW).toDispatchRequested();
        ownEvents.add(result);
        return result;
    }

    private DispatchClaim claim(DeliveryEvent request, Instant now) {
        return repository.claim(request, attemptId(request), PROVIDER, now, now.plusSeconds(30));
    }

    private DispatchService service(DispatchAttemptStore store, Instant now) {
        return new DispatchService(store, provider, new DispatchProperties(PROVIDER, Duration.ofSeconds(30)),
                Clock.fixed(now, ZoneOffset.UTC));
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
}

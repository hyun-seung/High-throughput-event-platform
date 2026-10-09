package messaging.pre.send;

import messaging.common.messages.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** Real DynamoDB conditions on a unique table; never reads or modifies the running AP's ORIGIN. */
@EnabledIfEnvironmentVariable(named = "DYNAMODB_TEST_ENDPOINT", matches = ".+")
class PreSendDecisionDynamoTest {
    private final String table = "pre_send_test_" + UUID.randomUUID().toString().replace("-", "");
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final Instant now = Instant.now();
    private final MessageSubmission admission = new MessageSubmission(UUID.randomUUID().toString(), 42L, "test-message",
            "01012345678", MessageCategory.GENERAL, Map.of("text", "hello"), null, now);
    private final PreSendReferenceReader references = mock(PreSendReferenceReader.class);
    private DynamoDbClient local;
    private DynamoDbClient isolated;
    private PreSendDecisionStore store;

    @BeforeEach
    void connect() {
        URI endpoint = URI.create(System.getenv("DYNAMODB_TEST_ENDPOINT"));
        if (!Set.of("localhost", "127.0.0.1").contains(endpoint.getHost())) {
            throw new IllegalArgumentException("Local test DynamoDB required");
        }
        local = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local"))).build();
        local.createTable(CreateTableRequest.builder().tableName(table).billingMode(BillingMode.PAY_PER_REQUEST)
                .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.S).build()).build());
        try (var waiter = local.waiter()) { waiter.waitUntilTableExists(b -> b.tableName(table)); }
        local.putItem(b -> b.tableName(table).item(MessageOriginCodec.encode(admission, mapper)));
        // Only redirect the table name; execute production GetItem/UpdateItem expressions on the real service.
        isolated = mock(DynamoDbClient.class);
        when(isolated.getItem(any(GetItemRequest.class))).thenAnswer(call ->
                local.getItem(call.<GetItemRequest>getArgument(0).toBuilder().tableName(table).build()));
        when(isolated.updateItem(any(UpdateItemRequest.class))).thenAnswer(call ->
                local.updateItem(call.<UpdateItemRequest>getArgument(0).toBuilder().tableName(table).build()));
        when(references.findContract(42L)).thenReturn(Optional.of(new ClientMessageContract(42L, true)));
        when(references.firstCarrier(any())).thenReturn(new CarrierResolution(HttpCarrier.KT, true));
        var clock = Clock.fixed(now.plusSeconds(1), ZoneOffset.UTC);
        store = new PreSendDecisionStore(isolated, mapper,
                new PreSendPreparation(references, clock, Duration.ofHours(3)), clock);
    }

    @AfterEach
    void cleanup() {
        if (local == null) return;
        try { local.deleteTable(b -> b.tableName(table)); }
        finally { local.close(); }
    }

    @Test
    void commandAndCarrierCommitTogetherAndReplayIgnoresCacheChanges() {
        var first = store.prepareOrLoad(admission).orElseThrow();
        assertStored(first);
        when(references.firstCarrier(any())).thenReturn(new CarrierResolution(HttpCarrier.LGU, false));
        when(references.findContract(42L)).thenReturn(Optional.empty());
        assertEquals(Optional.of(first), store.prepareOrLoad(admission));
        verify(isolated, times(2)).getItem(any(GetItemRequest.class));
        verify(isolated).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void concurrentDifferentSelectionsAllReturnOneFrozenCommand() throws Exception {
        var barrier = new CyclicBarrier(8);
        var reads = new AtomicInteger();
        var routes = new AtomicInteger();
        when(isolated.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
            var response = local.getItem(call.<GetItemRequest>getArgument(0).toBuilder().tableName(table).build());
            if (reads.incrementAndGet() <= 8) barrier.await(10, TimeUnit.SECONDS);
            return response;
        });
        when(references.firstCarrier(any())).thenAnswer(call ->
                new CarrierResolution(HttpCarrier.values()[routes.getAndIncrement() % 3], true));
        try (var workers = Executors.newFixedThreadPool(8)) {
            var tasks = new ArrayList<Future<Optional<PreSendDispatch>>>();
            for (int i = 0; i < 8; i++) tasks.add(workers.submit(() -> store.prepareOrLoad(admission)));
            var winner = tasks.getFirst().get(15, TimeUnit.SECONDS).orElseThrow();
            for (var task : tasks) assertEquals(Optional.of(winner), task.get(15, TimeUnit.SECONDS));
            assertStored(winner);
        }
    }

    @Test
    void legacyCarrierOnlyStateIsPreserved() {
        freezeLegacyCarrier();
        var result = store.prepareOrLoad(admission).orElseThrow();
        assertEquals(HttpCarrier.LGU, result.command().carrier());
        assertStored(result);
        verify(references, never()).firstCarrier(any());
    }

    @Test
    void concurrentLegacyCarrierWriteCannotBeOverwrittenAndIsUsedOnRetry() {
        var writes = new AtomicInteger();
        when(isolated.updateItem(any(UpdateItemRequest.class))).thenAnswer(call -> {
            if (writes.incrementAndGet() == 1) freezeLegacyCarrier();
            return local.updateItem(call.<UpdateItemRequest>getArgument(0).toBuilder().tableName(table).build());
        });
        assertThrows(IllegalStateException.class, () -> store.prepareOrLoad(admission));
        assertFalse(read().containsKey(PreSendDecisionStore.DISPATCH));
        var retry = store.prepareOrLoad(admission).orElseThrow();
        assertEquals(HttpCarrier.LGU, retry.command().carrier());
        assertStored(retry);
    }

    @Test
    void completionBetweenReadAndWritePreventsCommandPersistence() {
        when(isolated.updateItem(any(UpdateItemRequest.class))).thenAnswer(call -> {
            local.updateItem(b -> b.tableName(table).key(MessageOriginCodec.key(admission.clientMsgId()))
                    .updateExpression("SET completion_event_id = :id")
                    .expressionAttributeValues(Map.of(":id", AttributeValue.fromS("expiry-won"))));
            return local.updateItem(call.<UpdateItemRequest>getArgument(0).toBuilder().tableName(table).build());
        });
        assertTrue(store.prepareOrLoad(admission).isEmpty());
        assertFalse(read().containsKey(PreSendDecisionStore.DISPATCH));
        assertFalse(read().containsKey(PreSendDecisionStore.CARRIER));
    }

    @Test
    void rejectedContractStoresOnlyFailure() {
        when(references.findContract(42L)).thenReturn(Optional.empty());
        var result = store.prepareOrLoad(admission).orElseThrow();
        assertEquals(PreSendFailure.Reason.CONTRACT_MISSING, result.failure().reason());
        assertEquals(result, mapper.readValue(read().get(PreSendDecisionStore.DISPATCH).s(), PreSendDispatch.class));
        assertFalse(read().containsKey(PreSendDecisionStore.CARRIER));
    }

    private void freezeLegacyCarrier() {
        local.updateItem(b -> b.tableName(table).key(MessageOriginCodec.key(admission.clientMsgId()))
                .updateExpression("SET initial_http_carrier = :carrier, initial_http_carrier_mapped = :mapped")
                .expressionAttributeValues(Map.of(":carrier", AttributeValue.fromS("LGU"), ":mapped", AttributeValue.fromBool(false))));
    }
    private Map<String, AttributeValue> read() {
        return local.getItem(b -> b.tableName(table).key(MessageOriginCodec.key(admission.clientMsgId())).consistentRead(true)).item();
    }
    private void assertStored(PreSendDispatch expected) {
        var item = read();
        assertEquals(expected, mapper.readValue(item.get(PreSendDecisionStore.DISPATCH).s(), PreSendDispatch.class));
        assertEquals(expected.command().carrier().name(), item.get(PreSendDecisionStore.CARRIER).s());
        assertNotNull(item.get(PreSendDecisionStore.MAPPED).bool());
    }
}

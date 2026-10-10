package messaging.carrier.sender;

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
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** Executes the production transactions on isolated local tables, including changes after reservation. */
@EnabledIfEnvironmentVariable(named = "DYNAMODB_TEST_ENDPOINT", matches = ".+")
class CarrierReservationDynamoTest {
    private final String suffix = UUID.randomUUID().toString().replace("-", "");
    private final String originTable = "sender_origin_test_" + suffix;
    private final String stepTable = "sender_step_test_" + suffix;
    private final List<String> createdTables = new ArrayList<>();
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final Instant now = Instant.now();
    private final MessageSubmission admission = new MessageSubmission(UUID.randomUUID().toString(), 42L,
            "test-message", "01012345678", MessageCategory.GENERAL, Map.of("text", "hello"), null, now);
    private final HttpSendCommand first = new HttpSendCommand("first", HttpCarrier.SKT, 1, now.plusSeconds(3600),
            new HttpProviderRequest(admission.clientMsgId(), 42L, "GENERAL", admission.recipientNumber(), admission.payload(), now));
    private final HttpSendCommand retry = new HttpSendCommand("followup", HttpCarrier.KT, 1, first.deadlineAt(), first.request());
    private DynamoDbClient local;
    private DynamoDbClient isolated;
    private CarrierHttpAttemptStore store;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void connect() {
        URI endpoint = URI.create(System.getenv("DYNAMODB_TEST_ENDPOINT"));
        if (!Set.of("localhost", "127.0.0.1").contains(endpoint.getHost())) throw new IllegalArgumentException("Local DynamoDB required");
        local = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local"))).build();
        for (String table : List.of(originTable, stepTable)) {
            local.createTable(b -> b.tableName(table).billingMode(BillingMode.PAY_PER_REQUEST)
                    .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build())
                    .attributeDefinitions(AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.S).build()));
            createdTables.add(table);
            try (var waiter = local.waiter()) { waiter.waitUntilTableExists(b -> b.tableName(table)); }
        }
        var origin = new HashMap<>(MessageOriginCodec.encode(admission, mapper));
        origin.put("pre_send_dispatch", AttributeValue.fromS(mapper.writeValueAsString(new PreSendDispatch(first, null))));
        local.putItem(b -> b.tableName(originTable).item(origin));
        isolated = mock(DynamoDbClient.class);
        when(isolated.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
            var request = call.getArgument(0, GetItemRequest.class);
            return local.getItem(request.toBuilder().tableName(table(request.tableName())).build());
        });
        when(isolated.updateItem(any(UpdateItemRequest.class))).thenAnswer(call -> {
            var request = call.getArgument(0, UpdateItemRequest.class);
            return local.updateItem(request.toBuilder().tableName(table(request.tableName())).build());
        });
        when(isolated.transactWriteItems(any(Consumer.class))).thenAnswer(call -> {
            var builder = TransactWriteItemsRequest.builder();
            call.<Consumer<TransactWriteItemsRequest.Builder>>getArgument(0).accept(builder);
            var request = builder.build();
            var writes = request.transactItems().stream().map(item -> {
                var copy = item.toBuilder();
                if (item.conditionCheck() != null) copy.conditionCheck(item.conditionCheck().toBuilder().tableName(table(item.conditionCheck().tableName())).build());
                if (item.put() != null) copy.put(item.put().toBuilder().tableName(table(item.put().tableName())).build());
                if (item.update() != null) copy.update(item.update().toBuilder().tableName(table(item.update().tableName())).build());
                return copy.build();
            }).toList();
            return local.transactWriteItems(request.toBuilder().transactItems(writes).build());
        });
        store = new CarrierHttpAttemptStore(isolated, mapper);
    }

    @AfterEach
    void cleanup() {
        if (local == null) return;
        try { for (String table : createdTables) local.deleteTable(b -> b.tableName(table)); }
        finally { local.close(); }
    }

    @Test
    void initialSendReadsOnceButStillUsesBothTransactions() {
        var reservation = store.reserve(first, now);
        assertTrue(store.begin(reservation, now));
        verify(isolated).getItem(any(GetItemRequest.class));
        verify(isolated, times(2)).transactWriteItems(any(Consumer.class));
        assertEquals(CarrierHttpAttemptStore.State.SENDING, store.state(first));
    }

    @Test
    void followupReadsOriginAndAuthorizationOnlyAtReservation() {
        installFollowup();
        var reservation = store.reserve(retry, now.plusSeconds(60));
        assertTrue(store.begin(reservation, now.plusSeconds(61)));
        verify(isolated, times(2)).getItem(any(GetItemRequest.class));
        verify(isolated, times(2)).transactWriteItems(any(Consumer.class));
        assertEquals(CarrierHttpAttemptStore.State.SENDING, store.state(retry));
    }

    @Test
    void completionAfterReservationPreventsSending() {
        var reservation = store.reserve(first, now);
        changeOrigin("SET completion_event_id = :value", "completed");
        assertFalse(store.begin(reservation, now.plusSeconds(1)));
        assertEquals(CarrierHttpAttemptStore.State.PENDING, store.state(first));
    }

    @Test
    void changedInitialCommandPreventsSending() {
        var reservation = store.reserve(first, now);
        changeOrigin("SET pre_send_dispatch = :value", mapper.writeValueAsString(new PreSendDispatch(retry, null)));
        assertFalse(store.begin(reservation, now.plusSeconds(1)));
        assertEquals(CarrierHttpAttemptStore.State.PENDING, store.state(first));
    }

    @Test
    void newerFollowupDecisionPreventsSendingWithOldAuthorization() {
        installFollowup();
        var reservation = store.reserve(retry, now.plusSeconds(60));
        changeOrigin("SET result_decision_id = :value", "newer-result");
        assertFalse(store.begin(reservation, now.plusSeconds(61)));
        assertEquals(CarrierHttpAttemptStore.State.PENDING, store.state(retry));
    }

    @Test
    void modifiedFollowupAuthorizationPreventsSending() {
        installFollowup();
        var reservation = store.reserve(retry, now.plusSeconds(60));
        var modified = new FollowupHttpCommand("result-1", retry, now.plusSeconds(120));
        local.updateItem(b -> b.tableName(stepTable).key(FollowupHttpCommand.key(retry))
                .updateExpression("SET #auth = :value").expressionAttributeNames(Map.of("#auth", "authorization"))
                .expressionAttributeValues(Map.of(":value", AttributeValue.fromS(mapper.writeValueAsString(modified)))));
        assertFalse(store.begin(reservation, now.plusSeconds(61)));
        assertEquals(CarrierHttpAttemptStore.State.PENDING, store.state(retry));
    }

    @Test
    void duplicatePendingReservationCannotStartTwiceEvenIfRedisIsLost() {
        var one = store.reserve(first, now);
        var two = store.reserve(first, now);
        assertEquals(CarrierHttpAttemptStore.State.PENDING, two.state());
        assertTrue(store.begin(one, now));
        assertFalse(store.begin(two, now));
        assertEquals(CarrierHttpAttemptStore.State.SENDING, store.reserve(first, now).state());
    }

    @Test
    void observedResponseIsReusedOnRedelivery() {
        assertTrue(store.begin(store.reserve(first, now), now));
        var result = new CarrierHttpResult(CarrierHttpResult.id(first), admission.clientMsgId(), first.attemptId(),
                first.carrier(), first.invocation(), "HTTP_RESPONSE", CarrierHttpResult.Status.ACCEPTED,
                200, null, null, null, null, now);
        store.record(first, result);
        var replay = store.reserve(first, now);
        assertEquals(CarrierHttpAttemptStore.State.OBSERVED, replay.state());
        assertFalse(store.begin(replay, now));
        assertEquals(result, store.observation(first).orElseThrow());
    }

    @Test
    void deletedOriginCannotBeRecreatedOrSent() {
        var reservation = store.reserve(first, now);
        local.deleteItem(b -> b.tableName(originTable).key(MessageOriginCodec.key(admission.clientMsgId())));
        assertFalse(store.begin(reservation, now));
        assertEquals(CarrierHttpAttemptStore.State.PENDING, store.state(first));
        assertFalse(local.getItem(b -> b.tableName(originTable).key(MessageOriginCodec.key(admission.clientMsgId()))).hasItem());
    }

    private void installFollowup() {
        var followup = new FollowupHttpCommand("result-1", retry, now.plusSeconds(60));
        changeOrigin("SET result_decision_id = :value", followup.decisionId());
        var item = new HashMap<>(FollowupHttpCommand.key(retry));
        item.put("decision_id", AttributeValue.fromS(followup.decisionId()));
        item.put("authorization", AttributeValue.fromS(mapper.writeValueAsString(followup)));
        local.putItem(b -> b.tableName(stepTable).item(item));
    }
    private void changeOrigin(String update, String value) {
        local.updateItem(b -> b.tableName(originTable).key(MessageOriginCodec.key(admission.clientMsgId()))
                .updateExpression(update).expressionAttributeValues(Map.of(":value", AttributeValue.fromS(value))));
    }
    private String table(String original) {
        return switch (original) {
            case "ORIGIN" -> originTable;
            case "STEP" -> stepTable;
            default -> throw new IllegalArgumentException("Unexpected test table: " + original);
        };
    }
}

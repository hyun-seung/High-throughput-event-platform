package event.delivery.dispatch.lifecycle;

import event.common.delivery.*;
import event.common.dynamodb.config.DynamoDbTableInitializer;
import event.common.lifecycle.*;
import event.common.metrics.DeliveryMetrics;
import event.common.receipt.*;
import event.delivery.dispatch.config.*;
import event.delivery.dispatch.external.client.ProviderFailureException;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import event.delivery.dispatch.receipt.ReceiptResultRepository;
import event.delivery.dispatch.repository.DispatchAttemptRepository;
import event.delivery.dispatch.service.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.interceptor.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.*;
import java.util.*;

import static event.common.dynamodb.DynamoDbTableNames.*;
import static org.junit.jupiter.api.Assertions.*;

/** Counts real DB operations for serialized business paths, not AWS billing or scheduler replay. */
@EnabledIfEnvironmentVariable(named = "DYNAMODB_TEST_ENDPOINT", matches = ".+")
class DynamoDbWriteBudgetTest {
    static final Instant START = Instant.parse("2026-09-25T00:00:00Z");
    static final String PRIMARY = "budget-http", SECONDARY = "budget-tcp";
    final Budget budget = new Budget();
    final MutableClock clock = new MutableClock();
    final JsonMapper mapper = JsonMapper.builder().build();
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    final List<String> receiptIds = new ArrayList<>();
    final List<DeliveryFinalized> results = new ArrayList<>();
    final DispatchProperties config = new DispatchProperties(PRIMARY, Duration.ofSeconds(30), Duration.ofHours(3), Duration.ofSeconds(1));
    DynamoDbClient db;
    DeliveryEvent event;
    DispatchAttemptRepository attempts;
    ReceiptResultRepository receipts;
    LifecycleRepository lifecycle;
    LifecycleService service;

    @BeforeEach void setup() {
        var uri = URI.create(System.getenv("DYNAMODB_TEST_ENDPOINT"));
        if (!Set.of("localhost", "127.0.0.1").contains(uri.getHost())) throw new IllegalArgumentException("Local only");
        db = DynamoDbClient.builder().endpointOverride(uri).region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
                .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(10)).addExecutionInterceptor(budget)).build();
        new DynamoDbTableInitializer(db).run(null);
        attempts = new DispatchAttemptRepository(db);
        receipts = new ReceiptResultRepository(db, config, mapper);
        lifecycle = new LifecycleRepository(db, mapper);
        var metrics = new DeliveryMetrics(meters);
        var secondary = new SecondaryDispatchService(attempts, (e, id) -> new ProviderDispatchResponse(e.deliveryId(), true, clock.instant()),
                new SecondaryDispatchProperties(SECONDARY, Duration.ofHours(4)), config, clock, metrics);
        var primary = new DispatchService(attempts, (e, id) -> new ProviderDispatchResponse(e.deliveryId(), true, clock.instant()), config, clock, metrics, secondary);
        service = new LifecycleService(lifecycle, receipts, primary, secondary, config, results::add, clock, meters);
        budget.enabled = true;
    }

    // Ingress is another module: seed its single conditional META Put with the same relevant fields.
    void ingress(boolean fallback) {
        event = DeliveryEvent.requested(UUID.randomUUID().toString(), 999L, "SMS", Map.of("message", "budget-test"), START, fallback).toDispatchRequested();
        var item = new HashMap<>(DeliveryCompletion.metaKey(event.deliveryId()));
        item.put("delivery_id", AttributeValue.fromS(event.deliveryId()));
        item.put("event_id", AttributeValue.fromS(DeliveryIds.eventId(event.deliveryId(), DeliveryEventType.DELIVERY_REQUESTED)));
        item.put("status", AttributeValue.fromS("ACCEPTED"));
        item.put("tenant_id", AttributeValue.fromN("999"));
        item.put("delivery_type", AttributeValue.fromS("SMS"));
        item.put("payload", AttributeValue.fromS(mapper.writeValueAsString(event.payload())));
        item.put("occurred_at", AttributeValue.fromS(START.toString()));
        item.put("fallback_allowed", AttributeValue.fromBool(fallback));
        LifecycleIndex.add(item, event.deliveryId(), 0);
        db.putItem(r -> r.tableName(tableForKey(item)).item(item).conditionExpression("attribute_not_exists(pk)"));
    }

    @Test void normalPrimaryThroughCompaction() {
        ingress(false);
        invoke(1, 1, true, "DELIVERED");
        finish("normal_primary", 8, 14, 0);
    }

    @Test void eightNoResponseInvocationsWithoutReceipts() {
        ingress(true);
        for (int route = 1; route <= 2; route++) for (int invocation = 1; invocation <= 4; invocation++) {
            invoke(route, invocation, false, null);
            clock.now = clock.now.plusSeconds(1);
        }
        finish("eight_no_response", 23, 29, 6);
    }

    @Test void eightAcceptedInvocationsAndEightReceipts() {
        ingress(true);
        for (int route = 1; route <= 2; route++) for (int invocation = 1; invocation <= 4; invocation++) {
            invoke(route, invocation, true, invocation < 4 ? "RETRY_1S" : route == 1 ? "FALLBACK" : "DELIVERED");
            clock.now = clock.now.plusSeconds(1);
        }
        finish("eight_receipts", 31, 53, 6);
    }

    @Test void eightReceiptsThenReviewAndExpiryOnBothRoutes() {
        ingress(true);
        for (int route = 1; route <= 2; route++) {
            for (int invocation = 1; invocation <= 4; invocation++) {
                invoke(route, invocation, true, invocation < 4 ? "RETRY_1S" : "UNRECOGNIZED");
                clock.now = clock.now.plusSeconds(1);
            }
            var item = lifecycle.read(event.deliveryId(), "ATTEMPT#" + id(route));
            clock.now = LifecycleRepository.deadline(item);
            assertTrue(lifecycle.expire(event.deliveryId(), item, clock.now));
        }
        finish("eight_receipts_review_expiry", 33, 55, 6);
    }

    void invoke(int route, int invocation, boolean accepted, String code) {
        var claim = route == 1
                ? attempts.claim(event, id(1), PRIMARY, clock.now, clock.now.plusSeconds(30), START.plus(config.primaryTtl()))
                : attempts.claimSecondary(event, attempts.prepareSecondary(event, id(1), SECONDARY, Duration.ofHours(4)).orElseThrow(), clock.now, clock.now.plusSeconds(30));
        var attempt = claim.attempt();
        assertNotNull(attempt); assertEquals(invocation - 1, attempt.retryCount());
        if (accepted) attempts.markAccepted(attempt, clock.now, clock.now);
        else attempts.recordFailure(attempt, DispatchRetryPolicy.decide(attempt, ProviderFailureException.Kind.NO_RESPONSE, clock.now, config));
        if (code != null) {
            var receipt = ReceiptEvent.received(UUID.randomUUID().toString(), event.deliveryId(), id(route), route == 1 ? PRIMARY : SECONDARY, route,
                    code.equals("DELIVERED") ? ReceiptOutcome.DELIVERED : ReceiptOutcome.FAILED, code, clock.now, clock.now, invocation);
            receiptIds.add(receipt.eventId());
            receipts.apply(receipt, clock.now);
        }
    }

    void finish(String name, int calls, int mutations, int failedClaims) {
        service.reconcile(event.deliveryId());
        assertEquals(1, results.size());
        // SQL durability is tested elsewhere. This fixture explicitly invokes only the DDB compaction boundary.
        assertTrue(new DeliveryCompactor(db).compact(results.getFirst()));
        budget.enabled = false;
        assertEquals(calls, budget.writeCalls, name);
        assertEquals(mutations, budget.puts + budget.updates + budget.deletes, name);
        assertEquals(failedClaims, budget.failedWrites, name);
        if (event.schemaVersion() == 2) assertEquals(Map.of(ORIGIN, 3, STEP, mutations - 3), budget.mutationsByTable, name);
        assertEquals(event.fallbackAllowed() ? 2 : 1, budget.conditionChecks, name);
        System.out.println("DDB_WRITE_BUDGET " + name + " " + mapper.writeValueAsString(Map.of(
                "successfulWriteCalls", budget.writeCalls, "failedWriteCalls", budget.failedWrites,
                "putItems", budget.puts, "updateItems", budget.updates, "deleteItems", budget.deletes,
                "successfulConditionChecks", budget.conditionChecks, "successfulReadCalls", budget.readCalls, "mutationsByTable", budget.mutationsByTable)));
    }
    String id(int route) { return DeliveryIds.attemptId(event.deliveryId(), route == 1 ? PRIMARY : SECONDARY, route, 1); }

    @AfterEach void cleanup() {
        budget.enabled = false;
        if (event != null) {
            for (String sk : List.of("META", "FINAL", "ATTEMPT#" + id(1), "ATTEMPT#" + id(2)))
                db.deleteItem(r -> r.tableName(tableForKey(LifecycleRepository.key(event.deliveryId(), sk))).key(LifecycleRepository.key(event.deliveryId(), sk)));
            for (String id : receiptIds) db.deleteItem(r -> r.tableName(tableForKey(ReceiptResultRepository.receiptKey(id))).key(ReceiptResultRepository.receiptKey(id)));
        }
        db.close(); meters.close();
    }
    static class MutableClock extends Clock {
        Instant now = START;
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
        public Instant instant() { return now; }
    }
    static class Budget implements ExecutionInterceptor {
        boolean enabled;
        final Map<String, Integer> mutationsByTable = new HashMap<>();
        void changed(String table) { mutationsByTable.merge(table, 1, Integer::sum); }
        int writeCalls, failedWrites, puts, updates, deletes, conditionChecks, readCalls;
        @Override public void afterExecution(Context.AfterExecution ctx, ExecutionAttributes attrs) {
            if (!enabled) return;
            if (ctx.request() instanceof TransactWriteItemsRequest tx) {
                writeCalls++;
                for (var item : tx.transactItems()) {
                    if (item.put() != null) { puts++; changed(item.put().tableName()); }
                    if (item.update() != null) { updates++; changed(item.update().tableName()); }
                    if (item.delete() != null) { deletes++; changed(item.delete().tableName()); }
                    if (item.conditionCheck() != null) conditionChecks++;
                }
            } else if (ctx.request() instanceof PutItemRequest put) { writeCalls++; puts++; changed(put.tableName()); }
            else if (ctx.request() instanceof UpdateItemRequest update) { writeCalls++; updates++; changed(update.tableName()); }
            else if (ctx.request() instanceof DeleteItemRequest delete) { writeCalls++; deletes++; changed(delete.tableName()); }
            else if (ctx.request() instanceof GetItemRequest || ctx.request() instanceof QueryRequest) readCalls++;
        }
        @Override public void onExecutionFailure(Context.FailedExecution ctx, ExecutionAttributes attrs) {
            if (enabled && (ctx.request() instanceof TransactWriteItemsRequest || ctx.request() instanceof PutItemRequest
                    || ctx.request() instanceof UpdateItemRequest || ctx.request() instanceof DeleteItemRequest)) failedWrites++;
        }
    }
}

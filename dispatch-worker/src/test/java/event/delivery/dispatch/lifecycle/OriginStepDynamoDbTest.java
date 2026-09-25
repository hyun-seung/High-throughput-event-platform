package event.delivery.dispatch.lifecycle;

import event.common.delivery.*;
import event.common.lifecycle.*;
import event.common.receipt.*;
import event.delivery.dispatch.external.client.ProviderFailureException;
import event.delivery.dispatch.model.*;
import event.delivery.dispatch.retry.RetryCommand;
import event.delivery.dispatch.service.DispatchRetryPolicy;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import static event.common.dynamodb.DynamoDbTableNames.DELIVERY_STATE;
import static org.junit.jupiter.api.Assertions.*;

/** Same real-DB budget harness, now exercising the v2 ORIGIN/STEP contract. */
class OriginStepDynamoDbTest extends DynamoDbWriteBudgetTest {
    final Queue<RetryCommand> retries = new ArrayDeque<>();
    @Override void ingress(boolean fallback) {
        event = DeliveryEvent.requested(UUID.randomUUID().toString(), 999L, "SMS", Map.of("message", "budget-test"), START, fallback)
                .forAdmission().execution(UUID.randomUUID().toString()).toDispatchRequested();
        var item = new HashMap<>(DeliveryCompletion.metaKey(event.requestKey()));
        item.put("delivery_id", AttributeValue.fromS(event.deliveryId()));
        item.put("request_key", AttributeValue.fromS(event.requestKey()));
        item.put("schema_version", AttributeValue.fromN("2"));
        item.put("event_id", AttributeValue.fromS(DeliveryIds.eventId(event.deliveryId(), DeliveryEventType.DELIVERY_REQUESTED)));
        item.put("status", AttributeValue.fromS("ACCEPTED")); item.put("tenant_id", AttributeValue.fromN("999"));
        item.put("delivery_type", AttributeValue.fromS("SMS")); item.put("payload", AttributeValue.fromS(mapper.writeValueAsString(event.payload())));
        item.put("occurred_at", AttributeValue.fromS(START.toString())); item.put("fallback_allowed", AttributeValue.fromBool(fallback));
        LifecycleIndex.add(item, event.deliveryId(), 0);
        db.putItem(r -> r.tableName(DELIVERY_STATE).item(item).conditionExpression("attribute_not_exists(pk)"));
        receipts.retryPublisher(retries::add);
    }
    @Override void invoke(int route, int invocation, boolean accepted, String code) {
        DispatchClaim claim;
        if (invocation > 1) {
            var command = retries.remove(); assertEquals(invocation - 1, command.targetRetryCount());
            claim = attempts.claimRetry(command, clock.now, clock.now.plusSeconds(30));
        } else claim = route == 1
                ? attempts.claim(event, id(1), PRIMARY, clock.now, clock.now.plusSeconds(30), START.plus(config.primaryTtl()))
                : attempts.claimSecondary(event, attempts.prepareSecondary(event, id(1), SECONDARY, Duration.ofHours(4)).orElseThrow(), clock.now, clock.now.plusSeconds(30));
        var attempt = claim.attempt(); assertNotNull(attempt); assertEquals(invocation - 1, attempt.retryCount());
        if (accepted) attempts.markAccepted(attempt, clock.now, clock.now);
        else {
            var decision = DispatchRetryPolicy.decide(attempt, ProviderFailureException.Kind.NO_RESPONSE, clock.now, config);
            if (decision.nextAttemptAt() != null) retries.add(new RetryCommand(event, attempt, attempt.retryCount() + 1, decision.nextAttemptAt()));
            else attempts.recordFailure(attempt, decision);
        }
        if (code != null) {
            var receipt = ReceiptEvent.received(UUID.randomUUID().toString(), event.deliveryId(), id(route), route == 1 ? PRIMARY : SECONDARY, route,
                    code.equals("DELIVERED") ? ReceiptOutcome.DELIVERED : ReceiptOutcome.FAILED, code, clock.now, clock.now, invocation);
            receipts.apply(receipt, clock.now);
        }
    }
    @Override @Test void normalPrimaryThroughCompaction() { ingress(false); invoke(1, 1, true, "DELIVERED"); finish("v2_normal_primary", 5, 7, 0); }
    @Override @Test void eightNoResponseInvocationsWithoutReceipts() {
        ingress(true);
        for (int route = 1; route <= 2; route++) for (int invocation = 1; invocation <= 4; invocation++) {
            invoke(route, invocation, false, null); clock.now = clock.now.plusSeconds(1);
        }
        finish("v2_eight_no_response", 14, 18, 0);
    }
    @Override @Test void eightAcceptedInvocationsAndEightReceipts() {
        ingress(true);
        for (int route = 1; route <= 2; route++) for (int invocation = 1; invocation <= 4; invocation++) {
            invoke(route, invocation, true, invocation < 4 ? "RETRY_1S" : route == 1 ? "FALLBACK" : "DELIVERED");
            clock.now = clock.now.plusSeconds(1);
        }
        finish("v2_eight_receipts", 21, 25, 0);
    }
    @Override @Test void eightReceiptsThenReviewAndExpiryOnBothRoutes() {
        ingress(true);
        for (int route = 1; route <= 2; route++) {
            for (int invocation = 1; invocation <= 4; invocation++) {
                invoke(route, invocation, true, invocation < 4 ? "RETRY_1S" : "UNRECOGNIZED"); clock.now = clock.now.plusSeconds(1);
            }
            var item = lifecycle.read(event.deliveryId(), "ATTEMPT#" + id(route)); clock.now = LifecycleRepository.deadline(item);
            assertTrue(lifecycle.expire(event.deliveryId(), item, clock.now));
        }
        finish("v2_eight_receipts_review_expiry", 24, 28, 0);
    }
    @Test void concurrentRetryCommandsAdvanceOnceAndLateResultCannotOverwrite() throws Exception {
        ingress(false);
        var first = attempts.claim(event, id(1), PRIMARY, START, START.plusSeconds(30), START.plusSeconds(100)).attempt();
        var retry = new RetryCommand(event, first, 1, START.plusSeconds(1));
        var statuses = new ArrayList<DispatchClaimStatus>();
        try (var workers = Executors.newFixedThreadPool(8)) {
            var tasks = new ArrayList<Future<DispatchClaimStatus>>();
            for (int i = 0; i < 16; i++) tasks.add(workers.submit(() -> attempts.claimRetry(retry, START.plusSeconds(2), START.plusSeconds(32)).status()));
            for (var task : tasks) statuses.add(task.get(10, TimeUnit.SECONDS));
        }
        assertEquals(1, statuses.stream().filter(s -> s == DispatchClaimStatus.CLAIMED).count());
        assertThrows(software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException.class,
                () -> attempts.markAccepted(first, START, START));
        assertEquals("1", lifecycle.read(event.deliveryId(), "ATTEMPT#" + id(1)).get("retry_count").n());
    }
    @Test void cleanupAndNewGenerationFenceOldDispatchReceiptAndRepeatedCleanup() {
        ingress(false); invoke(1, 1, true, "DELIVERED"); service.reconcile(event.deliveryId());
        var old = event; var result = results.getFirst(); var compactor = new DeliveryCompactor(db);
        assertTrue(compactor.compact(result)); assertFalse(compactor.compact(result));
        assertTrue(lifecycle.read(old.requestKey(), "META").isEmpty());
        assertEquals(DispatchClaimStatus.ALREADY_ACCEPTED, attempts.claim(old, id(1), PRIMARY, START, START.plusSeconds(30), START.plusSeconds(100)).status());
        var newOrigin = new HashMap<>(DeliveryCompletion.metaKey(old.requestKey()));
        newOrigin.put("delivery_id", AttributeValue.fromS(UUID.randomUUID().toString()));
        db.putItem(r -> r.tableName(DELIVERY_STATE).item(newOrigin));
        assertFalse(compactor.compact(result));
        assertEquals(newOrigin, lifecycle.read(old.requestKey(), "META"));
        var receipt = ReceiptEvent.received(UUID.randomUUID().toString(), old.deliveryId(), id(1), PRIMARY, 1, ReceiptOutcome.DELIVERED, "DELIVERED", START, START, 1);
        assertEquals("late_discarded", receipts.apply(receipt, START).outcome());
        assertEquals(newOrigin, lifecycle.read(old.requestKey(), "META"));
    }
    @Test void receiptBeforeRetryClaimCancelsDurableCommand() {
        ingress(false); invoke(1, 1, true, "RETRY_1S"); var retry = retries.remove();
        var receipt = ReceiptEvent.received(UUID.randomUUID().toString(), event.deliveryId(), id(1), PRIMARY, 1, ReceiptOutcome.DELIVERED, "DELIVERED", START, START, 1);
        assertEquals("delivered", receipts.apply(receipt, START).outcome());
        assertEquals(DispatchClaimStatus.ALREADY_ACCEPTED, attempts.claimRetry(retry, START.plusSeconds(1), START.plusSeconds(31)).status());
    }
    @Test void duplicateSuccessArrivingAfterDeadlineIsOnlyLateDiscarded() {
        ingress(false);
        var claimed = attempts.claim(event, id(1), PRIMARY, START, START.plusSeconds(30), START.plus(config.primaryTtl())).attempt();
        attempts.markAccepted(claimed, START, START);
        var receipt = ReceiptEvent.received(UUID.randomUUID().toString(), event.deliveryId(), id(1), PRIMARY, 1,
                ReceiptOutcome.DELIVERED, "DELIVERED", START, START, 1);
        assertEquals("delivered", receipts.apply(receipt, START).outcome());
        var before = lifecycle.read(event.deliveryId(), "ATTEMPT#" + id(1));
        budget.enabled = false;
        assertEquals("late_discarded", receipts.apply(receipt, START.plus(config.primaryTtl())).outcome());
        assertEquals(before, lifecycle.read(event.deliveryId(), "ATTEMPT#" + id(1)));
    }

    @Test void lostCacheAndDispatchInputRecoverFromOriginKeyWithoutExtendingDeadline() {
        ingress(false);
        // The GSI yields the ORIGIN requestKey before any execution STEP exists.
        service.reconcile(event.requestKey());
        var step = lifecycle.read(event.deliveryId(), "ATTEMPT#" + id(1));
        assertEquals("ACCEPTED", step.get("status").s());
        assertEquals(START.plus(config.primaryTtl()), LifecycleRepository.deadline(step));
        clock.now = START.plus(config.primaryTtl()).plusSeconds(1);
        service.reconcile(event.requestKey());
        assertEquals(1, results.size()); assertEquals("EXPIRED", results.getFirst().outcome());
        assertEquals(START.plus(config.primaryTtl()), results.getFirst().resultAt());
    }

    @Test void failedRetryPublicationLeavesUnknownResultForReviewWithoutAnotherSend() {
        ingress(false);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var metrics = new event.common.metrics.DeliveryMetrics(meters);
        var secondary = new event.delivery.dispatch.service.SecondaryDispatchService(attempts, (e, id) -> { throw new AssertionError(); },
                new event.delivery.dispatch.config.SecondaryDispatchProperties(SECONDARY, Duration.ofHours(4)), config, clock, metrics);
        var primary = new event.delivery.dispatch.service.DispatchService(attempts, (e, id) -> {
            calls.incrementAndGet(); throw new ProviderFailureException(ProviderFailureException.Kind.NO_RESPONSE);
        }, config, clock, metrics, secondary);
        primary.retryPublisher(command -> { throw new IllegalStateException("Kafka unavailable"); });
        assertThrows(IllegalStateException.class, () -> primary.dispatch(event));
        assertEquals("0", lifecycle.read(event.deliveryId(), "ATTEMPT#" + id(1)).get("retry_count").n());
        clock.now = START.plusSeconds(31); primary.dispatch(event); assertEquals(1, calls.get());
    }

    @Test @org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable(named = "KAFKA_TEST_BOOTSTRAP_SERVERS", matches = ".+")
    void actualKafkaRetryReplayAfterConsumerRestartSendsOnlyOnce() throws Exception {
        ingress(false);
        var source = attempts.claim(event, id(1), PRIMARY, START, START.plusSeconds(30), START.plusSeconds(100)).attempt();
        var command = new RetryCommand(event, source, 1, START);
        String topic = "test.fixed-retry." + UUID.randomUUID();
        String servers = System.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS");
        var calls = new java.util.concurrent.atomic.AtomicInteger(); var handled = new java.util.concurrent.atomic.AtomicInteger();
        var metrics = new event.common.metrics.DeliveryMetrics(meters);
        var secondaryConfig = new event.delivery.dispatch.config.SecondaryDispatchProperties(SECONDARY, Duration.ofHours(4));
        var secondary = new event.delivery.dispatch.service.SecondaryDispatchService(attempts, (e, id) -> { throw new AssertionError(); }, secondaryConfig, config, clock, metrics);
        var primary = new event.delivery.dispatch.service.DispatchService(attempts, (e, id) -> {
            calls.incrementAndGet(); return new event.delivery.dispatch.external.dto.ProviderDispatchResponse(e.deliveryId(), true, clock.now);
        }, config, clock, metrics, secondary);
        var consumer = new event.delivery.dispatch.retry.RetryConsumer(attempts, primary, secondary, config, secondaryConfig, clock);
        var kafka = new org.springframework.boot.kafka.autoconfigure.KafkaProperties(); kafka.setBootstrapServers(List.of(servers));
        try (var admin = org.apache.kafka.clients.admin.Admin.create(Map.of("bootstrap.servers", servers));
             var producer = new event.delivery.dispatch.retry.RetryConfiguration.RetryTransport(kafka, mapper, topic)) {
            admin.createTopics(List.of(new org.apache.kafka.clients.admin.NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            try {
                var consumerConfig = new HashMap<String, Object>();
                consumerConfig.put("bootstrap.servers", servers); consumerConfig.put("group.id", topic); consumerConfig.put("auto.offset.reset", "earliest");
                consumerConfig.put("enable.auto.commit", false);
                var factory = new org.springframework.kafka.core.DefaultKafkaConsumerFactory<>(consumerConfig,
                        new org.apache.kafka.common.serialization.StringDeserializer(),
                        new org.springframework.kafka.support.serializer.JacksonJsonDeserializer<>(RetryCommand.class, false));
                for (int run = 0; run < 2; run++) {
                    var properties = new org.springframework.kafka.listener.ContainerProperties(topic);
                    properties.setAckMode(org.springframework.kafka.listener.ContainerProperties.AckMode.RECORD);
                    properties.setMessageListener((org.springframework.kafka.listener.MessageListener<String, RetryCommand>) record -> {
                        consumer.consume(record.value()); handled.incrementAndGet();
                    });
                    var container = new org.springframework.kafka.listener.KafkaMessageListenerContainer<>(factory, properties);
                    container.start();
                    try {
                        producer.publish(command); producer.publish(command);
                        long until = System.nanoTime() + Duration.ofSeconds(15).toNanos();
                        int expected = (run + 1) * 2;
                        while (handled.get() < expected && System.nanoTime() < until) Thread.sleep(20);
                        assertTrue(handled.get() >= expected); assertEquals(1, calls.get());
                    } finally { container.stop(); }
                }
                assertEquals("1", lifecycle.read(event.deliveryId(), "ATTEMPT#" + id(1)).get("retry_count").n());
            } finally { admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS); }
        }
    }

    @Override @AfterEach void cleanup() {
        budget.enabled = false;
        if (event != null) db.deleteItem(r -> r.tableName(DELIVERY_STATE).key(DeliveryCompletion.metaKey(event.requestKey())));
        super.cleanup();
    }
}

package event.delivery.dispatch.lifecycle;

import event.common.delivery.DeliveryIds;
import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.receipt.ReceiptResultRepository;
import event.delivery.dispatch.service.DispatchService;
import event.delivery.dispatch.service.SecondaryDispatchService;
import io.micrometer.core.instrument.MeterRegistry;

import java.time.Clock;
import java.util.Set;

import static event.common.dynamodb.DynamoDbAttributeNames.*;
import static event.delivery.dispatch.lifecycle.LifecycleRepository.text;

public class LifecycleService {
    private static final Set<String> FALLBACK = Set.of("FALLBACK_REQUIRED", "PRIMARY_EXPIRED", "RETRY_EXHAUSTED_NO_RESPONSE");
    private final LifecycleRepository repository;
    private final ReceiptResultRepository sources;
    private final DispatchService dispatch;
    private final SecondaryDispatchService secondary;
    private final DispatchProperties config;
    private final FinalizedPublisher publisher;
    private final Clock clock;
    private final MeterRegistry metrics;

    public LifecycleService(LifecycleRepository repository, ReceiptResultRepository sources, DispatchService dispatch,
                            SecondaryDispatchService secondary, DispatchProperties config, FinalizedPublisher publisher,
                            Clock clock, MeterRegistry metrics) {
        this.repository = repository; this.sources = sources; this.dispatch = dispatch; this.secondary = secondary;
        this.config = config; this.publisher = publisher; this.clock = clock; this.metrics = metrics;
    }

    private event.common.redis.DeliveryCache cache = event.common.redis.DeliveryCache.UNAVAILABLE;
    public void cache(event.common.redis.DeliveryCache cache) { this.cache = cache; }

    public void reconcile(String delivery) {
        var completed = repository.read(delivery, "FINAL");
        if (!completed.isEmpty()) { publish(delivery, completed); return; }
        var active = sources.loadDelivery(delivery);
        if (active.isEmpty()) { cache.removeSchedule(delivery); return; }
        var event = active.get();
        if (!delivery.equals(event.deliveryId())) cache.removeSchedule(delivery);
        delivery = event.deliveryId();
        completed = repository.read(delivery, "FINAL");
        if (!completed.isEmpty()) { publish(delivery, completed); return; }
        String parentId = DeliveryIds.attemptId(delivery, config.provider(), 1, 1);
        var parent = repository.read(delivery, "ATTEMPT#" + parentId);
        if (parent.isEmpty()) {
            repository.requireNoOtherPrimary(delivery, parentId);
            dispatch.dispatch(event);
            parent = repository.read(delivery, "ATTEMPT#" + parentId);
            if (parent.isEmpty()) throw new IllegalStateException("Dispatch did not persist attempt");
        }
        repository.releaseMeta(event.requestKey(), repository.read(event.requestKey(), "META"));
        if (repository.expire(delivery, parent, clock.instant())) {
            count("expired"); parent = repository.read(delivery, "ATTEMPT#" + parentId);
        } else {
            // Re-read after a racing receipt even if expiry lost the condition.
            parent = repository.read(delivery, "ATTEMPT#" + parentId);
        }
        String state = text(parent, STATUS);
        if (state.equals("RETRY_SCHEDULED")) { dispatch.dispatch(event); return; }
        var terminal = parent;
        if (state.equals("DECISION_PENDING") && event.fallbackAllowed() && FALLBACK.contains(text(parent, FAILURE_REASON))) {
            String childId = text(parent, "secondary_attempt_id");
            var child = childId.isEmpty() ? java.util.Map.<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue>of()
                    : repository.read(delivery, "ATTEMPT#" + childId);
            if (child.isEmpty()) {
                secondary.dispatch(event, parentId);
                parent = repository.read(delivery, "ATTEMPT#" + parentId);
                childId = text(parent, "secondary_attempt_id");
                if (childId.isEmpty()) throw new IllegalStateException("Missing secondary binding");
                child = repository.read(delivery, "ATTEMPT#" + childId);
                if (child.isEmpty()) throw new IllegalStateException("Missing secondary attempt");
            }
            if (repository.expire(delivery, child, clock.instant())) count("expired");
            child = repository.read(delivery, "ATTEMPT#" + childId);
            if (text(child, STATUS).equals("RETRY_SCHEDULED")) { secondary.dispatch(event, parentId); return; }
            if (event.schemaVersion() < 2) repository.waitForSecondary(delivery, parent, child);
            terminal = child;
        }
        if (!Set.of("DELIVERED", "DECISION_PENDING").contains(text(terminal, STATUS))) return;
        if (repository.finalizeDelivery(event, parent, terminal, clock.instant())) count("finalized");
        completed = repository.read(delivery, "FINAL");
        if (!completed.isEmpty()) publish(delivery, completed);
    }

    private void publish(String delivery, java.util.Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> item) {
        if (!"PENDING".equals(text(item, "publish_state"))) { cache.removeSchedule(delivery); return; }
        var result = repository.result(item);
        publisher.publish(result);
        // v2 SQL history + notification commit is the cleanup proof. No separate Kafka-ack DB write.
        // Until SQL deletes the STEP, the durable index can republish the same immutable result after a crash.
        if (result.schemaVersion() < 2) repository.published(delivery, text(item, "result_event"), clock.instant());
        cache.removeSchedule(delivery);
        count("published");
    }
    private void count(String outcome) { metrics.counter("delivery.lifecycle.events", "outcome", outcome).increment(); }
}

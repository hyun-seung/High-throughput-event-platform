package event.delivery.result;

import event.common.lifecycle.DeliveryFinalized;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.UUID;

@Service
public class FinalizedStore {
    public enum Outcome { STORED, DUPLICATE }
    public static class Conflict extends IllegalStateException {
        public Conflict() { super("Finalized delivery has conflicting immutable content"); }
    }
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final FinalizedCodec codec;
    private final MeterRegistry meters;

    public FinalizedStore(JdbcTemplate jdbc, PlatformTransactionManager manager, FinalizedCodec codec, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.codec = codec;
        this.meters = meters;
        transaction = new TransactionTemplate(manager);
        transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transaction.setTimeout(10);
    }

    public Outcome save(DeliveryFinalized event) {
        codec.validate(event);
        var sample = Timer.start(meters);
        String metric = "failed";
        try {
            var outcome = Objects.requireNonNull(transaction.execute(status -> saveInTransaction(event)));
            metric = outcome == Outcome.STORED ? "stored" : "duplicate";
            return outcome;
        } catch (Conflict e) {
            metric = "conflict";
            throw e;
        } finally {
            meters.counter("delivery.result.records", "outcome", metric).increment();
            sample.stop(meters.timer("delivery.result.store.duration", "outcome", metric));
        }
    }

    private Outcome saveInTransaction(DeliveryFinalized e) {
        var deliveryId = UUID.fromString(e.deliveryId());
        var eventId = UUID.fromString(e.eventId());
        int inserted = jdbc.update("""
                INSERT INTO delivery_history(delivery_id, result_event_id, tenant_id, delivery_type, outcome,
                    reason, route_order, attempt_id, provider, occurred_at, result_at, finalized_at, deadline, result_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb))
                ON CONFLICT (delivery_id) DO NOTHING
                """, deliveryId, eventId, e.tenantId(), e.deliveryType(), e.outcome(), e.reason(), e.routeOrder(),
                UUID.fromString(e.attemptId()), e.provider(), at(e.occurredAt()), at(e.resultAt()),
                at(e.finalizedAt()), at(e.deadline()), codec.encode(e));
        if (inserted == 0) {
            // A new statement at READ COMMITTED sees the concurrent winner after unique-index waiting.
            String saved = jdbc.queryForObject("SELECT result_json::text FROM delivery_history WHERE delivery_id = ?", String.class, deliveryId);
            if (!e.equals(codec.stored(saved))) throw new Conflict();
            Integer pending = jdbc.queryForObject("SELECT count(*) FROM customer_notification_outbox WHERE result_event_id = ?", Integer.class, eventId);
            if (pending == null || pending != 1) throw new IllegalStateException("Finalized history has no notification reservation");
            // Never reset a notification's progress, even when a Kafka offset was lost.
            return Outcome.DUPLICATE;
        }
        jdbc.update("INSERT INTO customer_notification_outbox(result_event_id) VALUES (?)", eventId);
        return Outcome.STORED;
    }

    private static OffsetDateTime at(java.time.Instant instant) { return instant.atOffset(ZoneOffset.UTC); }
}

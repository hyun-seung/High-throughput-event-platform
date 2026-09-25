package event.delivery.result.notification;

import event.common.notification.CustomerResultBatch;
import event.common.lifecycle.DeliveryFinalized;
import event.delivery.result.FinalizedCodec;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

public class NotificationRepository {
    public record Claim(UUID batchId, long tenantId, String url, String body, int size, int attempt, UUID token) {}
    private final JdbcTemplate jdbc;
    private final NamedParameterJdbcTemplate named;
    private final TransactionTemplate tx;
    private final FinalizedCodec codec = new FinalizedCodec();
    private final JsonMapper mapper = JsonMapper.builder().build();

    public NotificationRepository(JdbcTemplate jdbc, PlatformTransactionManager manager) {
        this.jdbc = jdbc;
        this.named = new NamedParameterJdbcTemplate(jdbc);
        tx = new TransactionTemplate(manager);
        tx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        tx.setTimeout(10);
    }

    public Optional<Claim> claim(long tenantId, NotificationProperties.Destination destination, NotificationProperties settings) {
        return Objects.requireNonNull(tx.execute(status -> {
            jdbc.update("INSERT INTO customer_notification_lane(tenant_id) VALUES (?) ON CONFLICT DO NOTHING", tenantId);
            var lane = jdbc.queryForList("SELECT tenant_id FROM customer_notification_lane WHERE tenant_id = ? FOR UPDATE SKIP LOCKED", Long.class, tenantId);
            if (lane.isEmpty()) return Optional.empty();
            var active = jdbc.queryForList("""
                    SELECT batch_id, destination_url, attempt_count, status,
                        (lease_until <= clock_timestamp()) AS due
                    FROM customer_notification_batch WHERE tenant_id = ? AND status = 'IN_FLIGHT' FOR UPDATE
                    """, tenantId);
            if (active.isEmpty()) {
                // A future retry must not make newer results wait through its entire backoff.
                active = jdbc.queryForList("""
                        SELECT batch_id, destination_url, attempt_count, status, true AS due
                        FROM customer_notification_batch WHERE tenant_id = ? AND status = 'PENDING'
                            AND destination_url = ? AND next_attempt_at <= clock_timestamp()
                        ORDER BY next_attempt_at, batch_id LIMIT 1 FOR UPDATE
                        """, tenantId, destination.url().toString());
            }
            UUID batchId;
            if (!active.isEmpty()) {
                var batch = active.getFirst();
                if (!Boolean.TRUE.equals(batch.get("due"))) return Optional.empty();
                batchId = (UUID) batch.get("batch_id");
                if (((Number) batch.get("attempt_count")).intValue() >= 21) {
                    exhaustUnconfirmed(batchId);
                    return Optional.empty();
                }
                // Never forward an old batch to a changed destination or expose a token to its old URL.
                if (!destination.url().toString().equals(batch.get("destination_url"))) return Optional.empty();
            } else {
                var rows = jdbc.queryForList("""
                        SELECT h.result_json::text AS result_json FROM customer_notification_outbox o
                        JOIN delivery_history h ON h.result_event_id = o.result_event_id
                        WHERE h.tenant_id = ? AND o.status = 'PENDING' AND o.batch_id IS NULL
                          AND o.attempt_count = 0 AND o.next_attempt_at <= clock_timestamp()
                        ORDER BY o.created_at, o.result_event_id LIMIT 100 FOR UPDATE OF o SKIP LOCKED
                        """, tenantId);
                if (rows.isEmpty()) return Optional.empty();
                batchId = UUID.randomUUID();
                List<DeliveryFinalized> results = new ArrayList<>();
                String body = null;
                for (var row : rows) {
                    results.add(codec.stored((String) row.get("result_json")));
                    String candidate = mapper.writeValueAsString(new CustomerResultBatch(1, batchId.toString(), tenantId, results));
                    if (candidate.getBytes(StandardCharsets.UTF_8).length > settings.maxBatchBytes()) {
                        results.removeLast();
                        break;
                    }
                    body = candidate;
                }
                if (results.isEmpty()) throw new IllegalStateException("Single customer result exceeds notification size limit");
                jdbc.update("""
                        INSERT INTO customer_notification_batch(batch_id, tenant_id, destination_url, request_body, item_count, status)
                        VALUES (?, ?, ?, ?, ?, 'PENDING')
                        """, batchId, tenantId, destination.url().toString(), body, results.size());
                int linked = named.update("UPDATE customer_notification_outbox SET batch_id = :batch, updated_at = clock_timestamp() WHERE result_event_id IN (:ids) AND batch_id IS NULL",
                        Map.of("batch", batchId, "ids", results.stream().map(r -> UUID.fromString(r.eventId())).toList()));
                if (linked != results.size()) throw new IllegalStateException("Notification batch membership changed");
            }
            UUID token = UUID.randomUUID();
            var claims = jdbc.query("""
                    UPDATE customer_notification_batch SET status = 'IN_FLIGHT', attempt_count = attempt_count + 1,
                        lease_token = ?, lease_until = clock_timestamp() + (? * interval '1 millisecond'), updated_at = clock_timestamp()
                    WHERE batch_id = ? AND attempt_count < 21
                    RETURNING batch_id, tenant_id, destination_url, request_body, item_count, attempt_count
                    """, (rs, n) -> new Claim(rs.getObject(1, UUID.class), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getInt(5), rs.getInt(6), token),
                    token, settings.lease().toMillis(), batchId);
            if (claims.size() != 1) throw new IllegalStateException("Notification reservation failed");
            var claim = claims.getFirst();
            int updated = jdbc.update("UPDATE customer_notification_outbox SET attempt_count = ?, updated_at = clock_timestamp() WHERE batch_id = ? AND status = 'PENDING'", claim.attempt(), batchId);
            if (updated != claim.size()) throw new IllegalStateException("Notification reservation is incomplete");
            return Optional.of(claim);
        }));
    }

    public boolean owns(Claim claim) {
        return Boolean.TRUE.equals(jdbc.queryForObject("""
                SELECT EXISTS (SELECT 1 FROM customer_notification_batch WHERE batch_id = ? AND status = 'IN_FLIGHT'
                    AND lease_token = ? AND attempt_count = ? AND lease_until > clock_timestamp())
                """, Boolean.class, claim.batchId(), claim.token(), claim.attempt()));
    }

    public boolean complete(Claim claim, boolean acknowledged, String error, Duration retryAfter) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            String next = acknowledged ? "DELIVERED" : claim.attempt() >= 21 ? "EXHAUSTED" : "PENDING";
            int changed = jdbc.update("""
                    UPDATE customer_notification_batch SET status = ?, lease_token = NULL, lease_until = NULL,
                        next_attempt_at = clock_timestamp() + (? * interval '1 millisecond'), last_error = ?, updated_at = clock_timestamp()
                    WHERE batch_id = ? AND status = 'IN_FLIGHT' AND lease_token = ? AND attempt_count = ? AND lease_until > clock_timestamp()
                    """, next, retryAfter.toMillis(), acknowledged ? null : error, claim.batchId(), claim.token(), claim.attempt());
            if (changed == 0) return false;
            int updated = jdbc.update("""
                    UPDATE customer_notification_outbox SET status = ?, next_attempt_at =
                        (SELECT next_attempt_at FROM customer_notification_batch WHERE batch_id = ?), updated_at = clock_timestamp()
                    WHERE batch_id = ? AND status = 'PENDING'
                    """, next, claim.batchId(), claim.batchId());
            if (updated != claim.size()) throw new IllegalStateException("Notification completion is incomplete");
            return true;
        }));
    }

    private void exhaustUnconfirmed(UUID batchId) {
        jdbc.update("""
                UPDATE customer_notification_batch SET status = 'EXHAUSTED', lease_token = NULL, lease_until = NULL,
                    last_error = 'LAST_ATTEMPT_UNCONFIRMED', updated_at = clock_timestamp() WHERE batch_id = ?
                """, batchId);
        jdbc.update("UPDATE customer_notification_outbox SET status = 'EXHAUSTED', updated_at = clock_timestamp() WHERE batch_id = ? AND status = 'PENDING'", batchId);
    }
}

package event.delivery.result.operations;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.*;

/** Requeues SQL cleanup only. The existing worker still checks completion evidence before deleting DynamoDB data. */
public final class CleanupOperations {
    public enum Outcome { QUEUED, ALREADY_QUEUED, ACTION_CONFLICT, STALE_OR_INELIGIBLE, BUSY }
    public record Request(long tenantId, UUID resultEventId, long expectedVersion, UUID actionId, String actor, String reason) {
        public Request {
            if (tenantId <= 0 || resultEventId == null || expectedVersion < 0 || actionId == null
                    || actor == null || !actor.matches("[A-Za-z0-9._@-]{1,100}")
                    || reason == null || reason.isBlank() || reason.length() > 500 || reason.chars().anyMatch(Character::isISOControl))
                throw new IllegalArgumentException("Tenant, result, version, action ID, actor and reason required");
        }
    }
    private final JdbcTemplate sql;
    private final TransactionTemplate tx;
    public CleanupOperations(JdbcTemplate sql, PlatformTransactionManager manager) {
        this.sql = sql; tx = new TransactionTemplate(manager); tx.setTimeout(5);
    }
    public Outcome retry(Request request) {
        return Objects.requireNonNull(tx.execute(status -> {
            // One action ID identifies one immutable operator intent, including after worker completion.
            if (!Boolean.TRUE.equals(sql.queryForObject("SELECT pg_try_advisory_xact_lock(?)", Boolean.class,
                    request.actionId().getMostSignificantBits()))) return Outcome.BUSY;
            var existing = sql.query("SELECT tenant_id,result_event_id,expected_version,action_id,actor,reason FROM delivery_cleanup_action WHERE action_id=?",
                    (r, n) -> new Request(r.getLong(1), r.getObject(2, UUID.class), r.getLong(3), r.getObject(4, UUID.class), r.getString(5), r.getString(6)), request.actionId());
            if (!existing.isEmpty()) return existing.getFirst().equals(request) ? Outcome.ALREADY_QUEUED : Outcome.ACTION_CONFLICT;
            int changed = sql.update("""
                    UPDATE delivery_cleanup_outbox c
                    SET next_attempt_at=clock_timestamp(), lease_token=NULL, lease_until=NULL, version=version+1
                    WHERE c.result_event_id=? AND c.version=? AND c.status='PENDING'
                      AND (c.lease_until IS NULL OR c.lease_until<=clock_timestamp())
                      AND EXISTS (SELECT 1 FROM delivery_history h JOIN customer_notification_outbox n ON n.result_event_id=h.result_event_id
                                  WHERE h.result_event_id=c.result_event_id AND h.tenant_id=?)
                    """, request.resultEventId(), request.expectedVersion(), request.tenantId());
            if (changed != 1) return Outcome.STALE_OR_INELIGIBLE;
            sql.update("INSERT INTO delivery_cleanup_action(action_id,tenant_id,result_event_id,expected_version,actor,reason) VALUES (?,?,?,?,?,?)",
                    request.actionId(), request.tenantId(), request.resultEventId(), request.expectedVersion(), request.actor(), request.reason());
            return Outcome.QUEUED;
        }));
    }
}

package event.delivery.result.operations;

import event.common.lifecycle.ManualResolution;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import java.time.*;
import java.util.*;

/** Commit intent before touching DynamoDB; keep unresolved evidence available across process failure. */
public final class ResolutionOperations {
    public enum Outcome { APPLIED, ALREADY_APPLIED, REJECTED, ACTION_CONFLICT, PENDING_RECONCILIATION }
    public record Request(ManualResolution.Target target, String actor, String reason) {
        public Request {
            Objects.requireNonNull(target);
            if (actor == null || !actor.matches("[A-Za-z0-9._@-]{1,100}") || reason == null || reason.isBlank()
                    || reason.length() > 500 || reason.chars().anyMatch(Character::isISOControl)) throw new IllegalArgumentException("Actor and reason required");
        }
    }
    public record Pending(UUID actionId, UUID requestKey, UUID deliveryId, UUID attemptId, long expectedVersion,
                          String decision, String actor, Instant createdAt) { }
    public record DueAction(long tenantId, UUID actionId) { }
    private final JdbcTemplate sql;
    private final TransactionTemplate tx;
    private final ManualResolution resolution;
    private final Clock clock;
    public ResolutionOperations(JdbcTemplate sql, PlatformTransactionManager manager, ManualResolution resolution, Clock clock) {
        this.sql = sql; this.resolution = resolution; this.clock = clock;
        tx = new TransactionTemplate(manager); tx.setTimeout(20); tx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }
    public Outcome resolve(Request request) {
        var t = request.target();
        // Separate committed transaction: a later rollback must never erase the operator's intent.
        tx.executeWithoutResult(status -> sql.update("""
                INSERT INTO delivery_resolution_action(action_id,tenant_id,request_key,delivery_id,attempt_id,expected_version,decision,actor,reason)
                VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT (action_id) DO NOTHING
                """, t.actionId(), t.tenantId(), t.requestKey(), t.deliveryId(), t.attemptId(), t.expectedVersion(), t.decision().name(), request.actor(), request.reason()));
        return Objects.requireNonNull(tx.execute(status -> {
            var row = sql.queryForMap("SELECT * FROM delivery_resolution_action WHERE action_id=? FOR UPDATE", t.actionId());
            if (!request.equals(request(row))) return Outcome.ACTION_CONFLICT;
            if (row.get("status").equals("APPLIED")) return Outcome.ALREADY_APPLIED;
            if (row.get("status").equals("REJECTED")) return Outcome.REJECTED;
            // Serialize same-action callers while checking/applying DDB; SDK or SQL errors leave PENDING.
            var applied = resolution.apply(t, clock.instant());
            if (applied == ManualResolution.Outcome.PENDING_RECONCILIATION) return Outcome.PENDING_RECONCILIATION;
            boolean rejected = applied == ManualResolution.Outcome.STALE_OR_INELIGIBLE;
            sql.update("UPDATE delivery_resolution_action SET status=?,completed_at=clock_timestamp() WHERE action_id=?",
                    rejected ? "REJECTED" : "APPLIED", t.actionId());
            return rejected ? Outcome.REJECTED : applied == ManualResolution.Outcome.ALREADY_APPLIED ? Outcome.ALREADY_APPLIED : Outcome.APPLIED;
        }));
    }
    public Outcome resume(long tenant, UUID actionId) {
        var rows = sql.queryForList("SELECT * FROM delivery_resolution_action WHERE tenant_id=? AND action_id=?", tenant, actionId);
        if (rows.isEmpty()) throw new IllegalArgumentException("Action not found for tenant");
        return resolve(request(rows.getFirst()));
    }
    /** Claim only an existing intent; a crash after this SQL update merely delays its next check. */
    public Optional<DueAction> claimDue() {
        var rows = sql.query("""
                WITH candidate AS (
                    SELECT action_id FROM delivery_resolution_action
                    WHERE status='PENDING' AND created_at <= clock_timestamp() - interval '30 seconds'
                      AND next_check_at <= clock_timestamp()
                    ORDER BY next_check_at, action_id LIMIT 1 FOR UPDATE SKIP LOCKED
                )
                UPDATE delivery_resolution_action AS action
                SET next_check_at = clock_timestamp() + interval '30 seconds', check_count = check_count + 1
                FROM candidate WHERE action.action_id = candidate.action_id
                RETURNING action.tenant_id, action.action_id
                """, (rs, row) -> new DueAction(rs.getLong(1), rs.getObject(2, UUID.class)));
        return rows.stream().findFirst();
    }
    public DeliveryOperations.Page<Pending> pending(long tenant, UUID after, int limit) {
        if (tenant <= 0 || limit < 1 || limit > 100) throw new IllegalArgumentException("Tenant > 0 and limit 1..100 required");
        var rows = sql.query("SELECT action_id,request_key,delivery_id,attempt_id,expected_version,decision,actor,created_at FROM delivery_resolution_action "
                        + "WHERE tenant_id=? AND status='PENDING' " + (after == null ? "" : "AND action_id>? ") + "ORDER BY action_id LIMIT ?",
                (r, n) -> new Pending(r.getObject(1, UUID.class), r.getObject(2, UUID.class), r.getObject(3, UUID.class), r.getObject(4, UUID.class),
                        r.getLong(5), r.getString(6), r.getString(7), r.getTimestamp(8).toInstant()),
                after == null ? new Object[]{tenant, limit + 1} : new Object[]{tenant, after, limit + 1});
        boolean more = rows.size() > limit; if (more) rows.removeLast();
        return new DeliveryOperations.Page<>(List.copyOf(rows), more ? rows.getLast().actionId() : null, more);
    }
    private static Request request(Map<String, Object> row) {
        return new Request(new ManualResolution.Target(((Number) row.get("tenant_id")).longValue(), (UUID) row.get("request_key"),
                (UUID) row.get("delivery_id"), (UUID) row.get("attempt_id"), ((Number) row.get("expected_version")).longValue(),
                (UUID) row.get("action_id"), ManualResolution.Decision.valueOf((String) row.get("decision"))), (String) row.get("actor"), (String) row.get("reason"));
    }
}

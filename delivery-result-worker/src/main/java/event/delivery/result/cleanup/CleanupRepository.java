package event.delivery.result.cleanup;

import event.common.lifecycle.DeliveryFinalized;
import event.delivery.result.FinalizedCodec;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.*;

public class CleanupRepository {
    public record Claim(DeliveryFinalized result, UUID token) {}
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final FinalizedCodec codec = new FinalizedCodec();
    public CleanupRepository(JdbcTemplate jdbc, PlatformTransactionManager manager) {
        this.jdbc = jdbc; tx = new TransactionTemplate(manager); tx.setTimeout(10);
    }
    public Optional<Claim> claim() {
        return Objects.requireNonNull(tx.execute(status -> {
            var rows = jdbc.queryForList("""
                    SELECT c.result_event_id, h.result_json::text AS result_json
                    FROM delivery_cleanup_outbox c JOIN delivery_history h ON h.result_event_id = c.result_event_id
                    JOIN customer_notification_outbox n ON n.result_event_id = c.result_event_id
                    WHERE c.status = 'PENDING' AND c.next_attempt_at <= clock_timestamp()
                      AND (c.lease_until IS NULL OR c.lease_until <= clock_timestamp())
                    ORDER BY c.next_attempt_at, c.result_event_id LIMIT 1 FOR UPDATE OF c SKIP LOCKED
                    """);
            if (rows.isEmpty()) return Optional.empty();
            var row = rows.getFirst(); var token = UUID.randomUUID();
            jdbc.update("UPDATE delivery_cleanup_outbox SET lease_token = ?, lease_until = clock_timestamp() + interval '60 seconds', version = version + 1 WHERE result_event_id = ?", token, row.get("result_event_id"));
            var result = codec.stored((String) row.get("result_json")); codec.validate(result);
            return Optional.of(new Claim(result, token));
        }));
    }
    public boolean done(Claim claim) {
        return jdbc.update("""
                UPDATE delivery_cleanup_outbox SET status = 'DONE', completed_at = clock_timestamp(), lease_token = NULL, lease_until = NULL, version = version + 1
                WHERE result_event_id = ? AND status = 'PENDING' AND lease_token = ?
                """, UUID.fromString(claim.result().eventId()), claim.token()) == 1;
    }
    public void retry(Claim claim) {
        jdbc.update("""
                UPDATE delivery_cleanup_outbox SET next_attempt_at = clock_timestamp() + interval '30 seconds', lease_token = NULL, lease_until = NULL, version = version + 1
                WHERE result_event_id = ? AND status = 'PENDING' AND lease_token = ?
                """, UUID.fromString(claim.result().eventId()), claim.token());
    }
}

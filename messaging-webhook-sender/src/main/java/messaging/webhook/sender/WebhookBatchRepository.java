package messaging.webhook.sender;

import messaging.common.messages.CustomerWebhookSendCommand;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Repository
public class WebhookBatchRepository {
    public record Claim(UUID batchId, long clientId, String url, String body, int size, int attempt, UUID token) { }

    private final JdbcTemplate jdbc;
    private final NamedParameterJdbcTemplate named;
    private final TransactionTemplate tx;
    private final JsonMapper mapper;

    public WebhookBatchRepository(JdbcTemplate jdbc, PlatformTransactionManager manager, JsonMapper mapper) {
        this.jdbc = jdbc;
        this.named = new NamedParameterJdbcTemplate(jdbc);
        this.mapper = mapper;
        tx = new TransactionTemplate(manager);
        tx.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        tx.setTimeout(10);
    }

    /** Kafka offset may advance only after this transaction commits. */
    public void capture(CustomerWebhookSendCommand command) {
        long clientId = command.finalized().submission().clientId();
        String clientMsgId = command.finalized().decision().clientMsgId();
        String json = mapper.writeValueAsString(command);
        UUID webhookId = UUID.fromString(command.webhookId());
        tx.executeWithoutResult(status -> {
            jdbc.update("INSERT INTO tbl_webhook_lane(client_id) VALUES (?) ON CONFLICT DO NOTHING", clientId);
            jdbc.update("""
                    INSERT INTO tbl_webhook_outbox(webhook_id, client_msg_id, client_id, command_json)
                    VALUES (?, ?, ?, ?::jsonb) ON CONFLICT (webhook_id) DO NOTHING
                    """, webhookId, clientMsgId, clientId, json);
            var existing = jdbc.queryForMap("""
                    SELECT client_msg_id, client_id, command_json::text AS command_json
                    FROM tbl_webhook_outbox WHERE webhook_id = ?
                    """, webhookId);
            if (!clientMsgId.equals(existing.get("client_msg_id"))
                    || clientId != ((Number) existing.get("client_id")).longValue()
                    || !mapper.readTree(json).equals(mapper.readTree((String) existing.get("command_json")))) {
                throw new IllegalStateException("Conflicting customer webhook command: " + webhookId);
            }
        });
    }

    public Optional<Claim> claim(long clientId, WebhookSenderProperties.Destination destination,
                                 WebhookSenderProperties settings) {
        return Objects.requireNonNull(tx.execute(status -> {
            var lane = jdbc.queryForList("SELECT client_id FROM tbl_webhook_lane WHERE client_id = ? FOR UPDATE SKIP LOCKED", Long.class, clientId);
            if (lane.isEmpty()) return Optional.empty();
            var inFlight = jdbc.queryForList("""
                    SELECT batch_id, destination_url, attempt_count, lease_until <= clock_timestamp() AS due
                    FROM tbl_webhook_batch WHERE client_id = ? AND status = 'IN_FLIGHT' FOR UPDATE
                    """, clientId);
            UUID batchId;
            if (!inFlight.isEmpty()) {
                var row = inFlight.getFirst();
                if (!Boolean.TRUE.equals(row.get("due"))) return Optional.empty();
                batchId = (UUID) row.get("batch_id");
                if (((Number) row.get("attempt_count")).intValue() >= 21) {
                    exhaustUnconfirmed(batchId);
                    return Optional.empty();
                }
                if (!destination.url().toString().equals(row.get("destination_url"))) return Optional.empty();
            } else {
                var pending = jdbc.queryForList("""
                        SELECT batch_id, destination_url, attempt_count
                        FROM tbl_webhook_batch WHERE client_id = ? AND status = 'PENDING'
                            AND next_attempt_at <= clock_timestamp()
                        ORDER BY next_attempt_at, batch_id LIMIT 1 FOR UPDATE
                        """, clientId);
                if (!pending.isEmpty()) {
                    var row = pending.getFirst();
                    if (!destination.url().toString().equals(row.get("destination_url"))) return Optional.empty();
                    batchId = (UUID) row.get("batch_id");
                } else {
                    var rows = jdbc.queryForList("""
                            SELECT webhook_id, command_json::text AS command_json, captured_at
                            FROM tbl_webhook_outbox WHERE client_id = ? AND status = 'PENDING' AND batch_id IS NULL
                            ORDER BY captured_at, webhook_id LIMIT 100 FOR UPDATE SKIP LOCKED
                            """, clientId);
                    if (rows.isEmpty()) return Optional.empty();
                    batchId = UUID.randomUUID();
                    var results = new ArrayList<CustomerWebhookBatch.Result>();
                    var ids = new ArrayList<UUID>();
                    String body = null;
                    boolean sizeLimitReached = false;
                    for (var row : rows) {
                        var command = mapper.readValue((String) row.get("command_json"), CustomerWebhookSendCommand.class);
                        results.add(CustomerWebhookBatch.Result.from(command));
                        String candidate = mapper.writeValueAsString(new CustomerWebhookBatch(1, batchId, clientId, results));
                        if (candidate.getBytes(StandardCharsets.UTF_8).length > settings.maxBatchBytes()) {
                            results.removeLast();
                            sizeLimitReached = true;
                            break;
                        }
                        ids.add((UUID) row.get("webhook_id"));
                        body = candidate;
                    }
                    if (results.isEmpty()) throw new IllegalStateException("Single customer webhook exceeds batch byte limit");
                    Instant oldest = ((java.sql.Timestamp) rows.getFirst().get("captured_at")).toInstant();
                    if (rows.size() < 100 && !sizeLimitReached
                            && oldest.plus(settings.batchWindow()).isAfter(Instant.now())) return Optional.empty();
                    jdbc.update("""
                            INSERT INTO tbl_webhook_batch(batch_id, client_id, destination_url, request_body, item_count, status)
                            VALUES (?, ?, ?, ?, ?, 'PENDING')
                            """, batchId, clientId, destination.url().toString(), body, results.size());
                    int linked = named.update("""
                            UPDATE tbl_webhook_outbox SET batch_id = :batch, updated_at = clock_timestamp()
                            WHERE webhook_id IN (:ids) AND batch_id IS NULL
                            """, Map.of("batch", batchId, "ids", ids));
                    if (linked != results.size()) throw new IllegalStateException("Customer webhook batch membership changed");
                }
            }
            UUID token = UUID.randomUUID();
            var claims = jdbc.query("""
                    UPDATE tbl_webhook_batch SET status = 'IN_FLIGHT', attempt_count = attempt_count + 1,
                        lease_token = ?, lease_until = clock_timestamp() + (? * interval '1 millisecond'),
                        updated_at = clock_timestamp()
                    WHERE batch_id = ? AND attempt_count < 21
                    RETURNING batch_id, client_id, destination_url, request_body, item_count, attempt_count
                    """, (rs, index) -> new Claim(rs.getObject(1, UUID.class), rs.getLong(2), rs.getString(3),
                    rs.getString(4), rs.getInt(5), rs.getInt(6), token), token, settings.lease().toMillis(), batchId);
            if (claims.size() != 1) throw new IllegalStateException("Customer webhook reservation failed");
            return Optional.of(claims.getFirst());
        }));
    }

    public boolean owns(Claim claim) {
        return Boolean.TRUE.equals(jdbc.queryForObject("""
                SELECT EXISTS (SELECT 1 FROM tbl_webhook_batch WHERE batch_id = ? AND status = 'IN_FLIGHT'
                    AND lease_token = ? AND attempt_count = ? AND lease_until > clock_timestamp())
                """, Boolean.class, claim.batchId(), claim.token(), claim.attempt()));
    }

    /** Persist each HTTP attempt and its resulting state in one transaction. */
    public boolean complete(Claim claim, CustomerWebhookClient.Result result, Duration retryAfter) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            jdbc.update("""
                    INSERT INTO tbl_webhook_hist(batch_id, attempt_count, client_id, destination_url,
                        item_count, sent_at, http_status, acknowledged, error)
                    VALUES (?, ?, ?, ?, ?, clock_timestamp(), ?, ?, ?)
                    ON CONFLICT (batch_id, attempt_count) DO NOTHING
                    """, claim.batchId(), claim.attempt(), claim.clientId(), claim.url(), claim.size(),
                    result.httpStatus(), result.acknowledged(), result.error());
            String next = result.acknowledged() ? "DELIVERED" : claim.attempt() >= 21 ? "EXHAUSTED" : "PENDING";
            int changed = jdbc.update("""
                    UPDATE tbl_webhook_batch SET status = ?, lease_token = NULL, lease_until = NULL,
                        next_attempt_at = clock_timestamp() + (? * interval '1 millisecond'), last_error = ?,
                        updated_at = clock_timestamp()
                    WHERE batch_id = ? AND status = 'IN_FLIGHT' AND lease_token = ? AND attempt_count = ?
                    """, next, retryAfter.toMillis(), result.error(), claim.batchId(), claim.token(), claim.attempt());
            if (changed == 0) return false;
            if (!"PENDING".equals(next)) {
                int updated = jdbc.update("""
                        UPDATE tbl_webhook_outbox SET status = ?, updated_at = clock_timestamp()
                        WHERE batch_id = ? AND status = 'PENDING'
                        """, next, claim.batchId());
                if (updated != claim.size()) throw new IllegalStateException("Customer webhook completion is incomplete");
            }
            return true;
        }));
    }

    private void exhaustUnconfirmed(UUID batchId) {
        jdbc.update("""
                INSERT INTO tbl_webhook_hist(batch_id, attempt_count, client_id, destination_url,
                    item_count, sent_at, http_status, acknowledged, error)
                SELECT batch_id, attempt_count, client_id, destination_url, item_count,
                    clock_timestamp(), NULL, false, 'LAST_ATTEMPT_UNCONFIRMED'
                FROM tbl_webhook_batch WHERE batch_id = ?
                ON CONFLICT (batch_id, attempt_count) DO NOTHING
                """, batchId);
        jdbc.update("""
                UPDATE tbl_webhook_batch SET status = 'EXHAUSTED', lease_token = NULL, lease_until = NULL,
                    last_error = 'LAST_ATTEMPT_UNCONFIRMED', updated_at = clock_timestamp() WHERE batch_id = ?
                """, batchId);
        jdbc.update("UPDATE tbl_webhook_outbox SET status = 'EXHAUSTED', updated_at = clock_timestamp() WHERE batch_id = ? AND status = 'PENDING'", batchId);
    }
}

package event.delivery.ingress.dlt;

import event.common.delivery.DeliveryEvent;
import tools.jackson.databind.json.JsonMapper;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.UUID;
import java.util.function.Supplier;

/** Durable operations outbox. The session lock serializes operators; Kafka/SQL are not atomic. */
public final class DltRecoveryStore implements DltRecoveryPlanner.HistoryReader {
    @FunctionalInterface public interface Connections { Connection open() throws SQLException; }
    @FunctionalInterface public interface Publisher { Ack send(DeliveryEvent command) throws Exception; }
    public record Ack(String topic, int partition, long offset) { }
    public record Result(UUID operationId, String status) { }
    private final Connections connections;
    private final String schema;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public DltRecoveryStore(Connections connections, String schema) {
        if (schema == null || !schema.matches("[a-z][a-z0-9_]{0,62}")) throw new IllegalArgumentException("Invalid SQL schema");
        this.connections = connections; this.schema = schema;
    }

    @Override public DltRecoveryPlanner.History read(long tenant, String key, String execution) {
        String sql = "SELECT count(*) > 0, coalesce(bool_or(delivery_id = ?::uuid), false), "
                + "(SELECT complete_since FROM " + schema + ".dlt_history_coverage WHERE singleton AND origin_restore_enabled) FROM " + schema
                + ".delivery_history WHERE tenant_id = ? AND COALESCE(result_json->>'requestKey', delivery_id::text) = ?";
        try (var connection = connections.open(); var statement = connection.prepareStatement(sql)) {
            statement.setQueryTimeout(5);
            statement.setString(1, execution); statement.setLong(2, tenant); statement.setString(3, key);
            try (var result = statement.executeQuery()) {
                result.next();
                var coverage = result.getTimestamp(3);
                return new DltRecoveryPlanner.History(result.getBoolean(1), result.getBoolean(2),
                        coverage == null ? null : coverage.toInstant());
            }
        } catch (SQLException unavailable) { throw new IllegalStateException("SQL history unavailable", unavailable); }
    }

    public Result apply(String cluster, String targetTopic, DltRecoveryPlanner.Plan plan, String actor, String reason,
                        Supplier<DltRecoveryPlanner.Plan> recheck, Publisher publisher) {
        if (cluster == null || !cluster.matches("[A-Za-z0-9._-]{1,100}") || targetTopic == null
                || !targetTopic.matches("[A-Za-z0-9._-]{1,249}") || actor == null || !actor.matches("[A-Za-z0-9._@-]{1,100}")
                || reason == null || reason.isBlank() || reason.length() > 500 || reason.chars().anyMatch(Character::isISOControl)
                || !plan.eligible()) throw new IllegalArgumentException("Eligible plan, cluster, actor and reason required");
        var dlt = plan.preview().dlt();
        var source = dlt.source();
        UUID operation = UUID.nameUUIDFromBytes((cluster + ":" + source.topic() + ":" + source.partition() + ":" + source.offset())
                .getBytes(StandardCharsets.UTF_8));
        String command = mapper.writeValueAsString(plan.command());
        try (var connection = connections.open()) {
            try (var lock = connection.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
                lock.setLong(1, operation.getMostSignificantBits());
                try (var result = lock.executeQuery()) { result.next(); if (!result.getBoolean(1)) return new Result(operation, "BUSY"); }
            }
            try {
                // Connection closure releases the session lock after a process crash. Normal exits unlock explicitly.
                try (var insert = connection.prepareStatement("INSERT INTO " + schema + ".dlt_recovery_operation "
                        + "(operation_id,cluster_alias,source_topic,source_partition,source_offset,value_sha256,execution_id,target_topic,command_json,state) "
                        + "VALUES (?,?,?,?,?,?,?::uuid,?,?,'PENDING') ON CONFLICT DO NOTHING")) {
                    insert.setObject(1, operation); insert.setString(2, cluster); insert.setString(3, source.topic());
                    insert.setInt(4, source.partition()); insert.setLong(5, source.offset()); insert.setString(6, dlt.valueSha256());
                    insert.setString(7, plan.preview().executionId()); insert.setString(8, targetTopic); insert.setString(9, command);
                    insert.executeUpdate();
                }
                try (var query = connection.prepareStatement("SELECT state,value_sha256,execution_id,target_topic,command_json FROM "
                        + schema + ".dlt_recovery_operation WHERE operation_id=?")) {
                    query.setObject(1, operation);
                    try (var row = query.executeQuery()) {
                        if (!row.next() || !dlt.valueSha256().equals(row.getString(2))
                                || !plan.preview().executionId().equals(row.getString(3)) || !targetTopic.equals(row.getString(4))
                                || !command.equals(row.getString(5))) throw new IllegalStateException("Recovery identity conflict");
                        if ("ACKNOWLEDGED".equals(row.getString(1))) return new Result(operation, "ALREADY_ACKNOWLEDGED");
                    }
                }
                UUID attempt = UUID.randomUUID();
                connection.setAutoCommit(false);
                try (var update = connection.prepareStatement("UPDATE " + schema
                        + ".dlt_recovery_operation SET last_attempt=?,state='PENDING',updated_at=clock_timestamp() WHERE operation_id=?");
                     var insert = connection.prepareStatement("INSERT INTO " + schema
                             + ".dlt_recovery_attempt(attempt_id,operation_id,actor,reason,outcome) VALUES (?,?,?,?,'STARTED')")) {
                    update.setObject(1, attempt); update.setObject(2, operation); update.executeUpdate();
                    insert.setObject(1, attempt); insert.setObject(2, operation); insert.setString(3, actor); insert.setString(4, reason);
                    insert.executeUpdate(); connection.commit();
                } catch (SQLException failure) { connection.rollback(); throw failure; }
                finally { connection.setAutoCommit(true); }
                try {
                    var current = recheck.get();
                    if (!current.eligible() || !command.equals(mapper.writeValueAsString(current.command()))) {
                        finish(connection, operation, attempt, "HELD", "STATE_CHANGED", null);
                        return new Result(operation, "HELD_STATE_CHANGED");
                    }
                    var ack = publisher.send(plan.command());
                    if (ack == null || !targetTopic.equals(ack.topic()) || ack.partition() < 0 || ack.offset() < 0) {
                        throw new IllegalStateException("Invalid Kafka acknowledgement");
                    }
                    finish(connection, operation, attempt, "ACKNOWLEDGED", "ACKNOWLEDGED", ack);
                    return new Result(operation, "ACKNOWLEDGED");
                } catch (Exception uncertain) {
                    // The record may already exist in Kafka. Retain the identical execution command for a later retry.
                    finish(connection, operation, attempt, "PENDING", "UNCONFIRMED", null);
                    if (uncertain instanceof InterruptedException) Thread.currentThread().interrupt();
                    return new Result(operation, "UNCONFIRMED");
                }
            } finally {
                try (var unlock = connection.prepareStatement("SELECT pg_advisory_unlock(?)")) {
                    unlock.setLong(1, operation.getMostSignificantBits()); unlock.execute();
                }
            }
        } catch (SQLException unavailable) { throw new IllegalStateException("SQL recovery handoff unavailable", unavailable); }
    }

    private void finish(Connection connection, UUID operation, UUID attempt, String state, String outcome, Ack ack) throws SQLException {
        connection.setAutoCommit(false);
        try (var update = connection.prepareStatement("UPDATE " + schema + ".dlt_recovery_operation SET state=?,"
                + "target_partition=?,target_offset=?,updated_at=clock_timestamp() WHERE operation_id=? AND last_attempt=? AND state <> 'ACKNOWLEDGED'");
             var audit = connection.prepareStatement("UPDATE " + schema
                     + ".dlt_recovery_attempt SET outcome=?,finished_at=clock_timestamp() WHERE attempt_id=?")) {
            update.setString(1, state); update.setObject(2, ack == null ? null : ack.partition());
            update.setObject(3, ack == null ? null : ack.offset()); update.setObject(4, operation); update.setObject(5, attempt);
            if (update.executeUpdate() != 1) throw new SQLException("Recovery attempt superseded");
            audit.setString(1, outcome); audit.setObject(2, attempt); audit.executeUpdate(); connection.commit();
        } catch (SQLException failure) { connection.rollback(); throw failure; }
        finally { connection.setAutoCommit(true); }
    }
}

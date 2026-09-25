package event.delivery.ingress.dlt;

import java.sql.*;
import java.time.Instant;
import java.util.*;

/** Local SQL operations: metadata-only reads and audited optimistic recheck, never direct provider calls. */
public final class DltOperations {
    public record Item(UUID intakeId, String topic, int partition, long offset, String state, String decision,
                       UUID operationId, String recoveryState, Instant createdAt, Instant updatedAt) { }
    public record Page(List<Item> records, Long nextAfterOffset, boolean hasMore) { }
    public record Attempt(UUID attemptId, String actor, String reason, String outcome, Instant startedAt, Instant finishedAt) { }
    public record Action(UUID actionId, String actor, String reason, Instant expectedUpdatedAt, Instant queuedAt) { }
    public record Status(Item record, Long nextScanOffset, List<Attempt> recentAttempts, List<Action> recentActions) { }
    public record Recheck(UUID actionId, String status) { }
    private final DltRecoveryStore.Connections connections;
    private final String schema, cluster;
    private static final String COLUMNS = "i.intake_id,i.topic,i.partition_id,i.record_offset,i.state,i.decision,"
            + "i.operation_id,o.state,i.created_at,i.updated_at";
    public DltOperations(DltRecoveryStore.Connections connections, String schema, String cluster) {
        if (schema == null || !schema.matches("[a-z][a-z0-9_]{0,62}") || cluster == null || !cluster.matches("[A-Za-z0-9._-]{1,100}"))
            throw new IllegalArgumentException("Invalid operations scope");
        this.connections = connections; this.schema = schema; this.cluster = cluster;
    }
    private String joined() { return schema + ".dlt_intake_record i LEFT JOIN " + schema + ".dlt_recovery_operation o ON o.operation_id=i.operation_id"; }
    private static Instant instant(ResultSet row, int column) throws SQLException {
        var timestamp = row.getTimestamp(column); return timestamp == null ? null : timestamp.toInstant();
    }
    private static Item item(ResultSet row) throws SQLException {
        return new Item(row.getObject(1, UUID.class), row.getString(2), row.getInt(3), row.getLong(4), row.getString(5), row.getString(6),
                row.getObject(7, UUID.class), row.getString(8), instant(row, 9), instant(row, 10));
    }
    public Page held(String topic, int partition, long afterOffset, int limit) {
        if (topic == null || !topic.matches("[A-Za-z0-9._-]{1,249}") || partition < 0 || afterOffset < -1 || limit < 1 || limit > 100)
            throw new IllegalArgumentException("Topic, partition, afterOffset >= -1 and limit 1..100 required");
        try (var connection = connections.open()) {
            connection.setReadOnly(true);
            try (var query = connection.prepareStatement("SELECT " + COLUMNS + " FROM " + joined()
                    + " WHERE i.cluster_alias=? AND i.topic=? AND i.partition_id=? AND i.state='HELD' AND i.record_offset>? ORDER BY i.record_offset LIMIT ?")) {
                query.setQueryTimeout(5); query.setString(1, cluster); query.setString(2, topic); query.setInt(3, partition);
                query.setLong(4, afterOffset); query.setInt(5, limit + 1);
                var items = new ArrayList<Item>();
                try (var rows = query.executeQuery()) { while (rows.next()) items.add(item(rows)); }
                boolean more = items.size() > limit;
                if (more) items.removeLast();
                return new Page(List.copyOf(items), more ? items.getLast().offset() : null, more);
            }
        } catch (SQLException failure) { throw new IllegalStateException("SQL operations lookup unavailable", failure); }
    }
    public Status status(UUID intakeId) {
        try (var connection = connections.open()) {
            connection.setReadOnly(true);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            try {
                Item item;
                try (var query = connection.prepareStatement("SELECT " + COLUMNS + " FROM " + joined() + " WHERE i.cluster_alias=? AND i.intake_id=?")) {
                    query.setQueryTimeout(5); query.setString(1, cluster); query.setObject(2, intakeId);
                    try (var row = query.executeQuery()) { if (!row.next()) { connection.commit(); return null; } item = item(row); }
                }
                Long cursor = null;
                try (var query = connection.prepareStatement("SELECT next_offset FROM " + schema + ".dlt_intake_cursor WHERE cluster_alias=? AND topic=? AND partition_id=?")) {
                    query.setQueryTimeout(5); query.setString(1, cluster); query.setString(2, item.topic()); query.setInt(3, item.partition());
                    try (var row = query.executeQuery()) { if (row.next()) cursor = row.getLong(1); }
                }
                var attempts = new ArrayList<Attempt>();
                try (var query = connection.prepareStatement("SELECT attempt_id,actor,reason,outcome,started_at,finished_at FROM " + schema
                        + ".dlt_recovery_attempt WHERE operation_id=? ORDER BY started_at DESC,attempt_id LIMIT 20")) {
                    query.setQueryTimeout(5); query.setObject(1, item.operationId());
                    try (var rows = query.executeQuery()) { while (rows.next()) attempts.add(new Attempt(rows.getObject(1, UUID.class), rows.getString(2),
                            rows.getString(3), rows.getString(4), instant(rows, 5), instant(rows, 6))); }
                }
                var actions = new ArrayList<Action>();
                try (var query = connection.prepareStatement("SELECT action_id,actor,reason,expected_updated_at,queued_at FROM " + schema
                        + ".dlt_intake_action WHERE intake_id=? ORDER BY queued_at DESC,action_id LIMIT 20")) {
                    query.setQueryTimeout(5); query.setObject(1, intakeId);
                    try (var rows = query.executeQuery()) { while (rows.next()) actions.add(new Action(rows.getObject(1, UUID.class), rows.getString(2),
                            rows.getString(3), instant(rows, 4), instant(rows, 5))); }
                }
                connection.commit(); return new Status(item, cursor, List.copyOf(attempts), List.copyOf(actions));
            } catch (SQLException | RuntimeException failure) { connection.rollback(); throw failure; }
        } catch (SQLException failure) { throw new IllegalStateException("SQL operation detail unavailable", failure); }
    }
    public Recheck recheck(UUID intakeId, Instant expectedUpdatedAt, UUID actionId, String actor, String reason) {
        if (intakeId == null || actionId == null || expectedUpdatedAt == null || expectedUpdatedAt.getNano() % 1_000 != 0
                || actor == null || !actor.matches("[A-Za-z0-9._@-]{1,100}")
                || reason == null || reason.isBlank() || reason.length() > 500 || reason.chars().anyMatch(Character::isISOControl))
            throw new IllegalArgumentException("Version, action ID, actor and reason required");
        try (var connection = connections.open()) {
            connection.setAutoCommit(false);
            try {
                try (var lock = connection.prepareStatement("SELECT pg_try_advisory_xact_lock(?)")) {
                    lock.setLong(1, actionId.getMostSignificantBits());
                    try (var row = lock.executeQuery()) { row.next(); if (!row.getBoolean(1)) { connection.rollback(); return new Recheck(actionId, "BUSY"); } }
                }
                try (var query = connection.prepareStatement("SELECT a.intake_id,a.expected_updated_at,a.actor,a.reason,i.cluster_alias FROM " + schema
                        + ".dlt_intake_action a JOIN " + schema + ".dlt_intake_record i ON i.intake_id=a.intake_id WHERE a.action_id=?")) {
                    query.setObject(1, actionId);
                    try (var row = query.executeQuery()) {
                        if (row.next()) {
                            boolean same = intakeId.equals(row.getObject(1, UUID.class)) && expectedUpdatedAt.equals(instant(row, 2))
                                    && actor.equals(row.getString(3)) && reason.equals(row.getString(4)) && cluster.equals(row.getString(5));
                            connection.commit(); return new Recheck(actionId, same ? "ALREADY_QUEUED" : "ACTION_CONFLICT");
                        }
                    }
                }
                try (var update = connection.prepareStatement("UPDATE " + schema + ".dlt_intake_record SET state='NEW',decision='OPERATOR_RECHECK',"
                        + "updated_at=clock_timestamp() WHERE intake_id=? AND cluster_alias=? AND state='HELD' AND updated_at=?")) {
                    update.setObject(1, intakeId); update.setString(2, cluster); update.setTimestamp(3, Timestamp.from(expectedUpdatedAt));
                    if (update.executeUpdate() != 1) { connection.rollback(); return new Recheck(actionId, "STALE_OR_NOT_HELD"); }
                }
                try (var insert = connection.prepareStatement("INSERT INTO " + schema
                        + ".dlt_intake_action(action_id,intake_id,expected_updated_at,actor,reason) VALUES (?,?,?,?,?)")) {
                    insert.setObject(1, actionId); insert.setObject(2, intakeId); insert.setTimestamp(3, Timestamp.from(expectedUpdatedAt));
                    insert.setString(4, actor); insert.setString(5, reason); insert.executeUpdate();
                }
                connection.commit(); return new Recheck(actionId, "QUEUED");
            } catch (SQLException | RuntimeException failure) { connection.rollback(); throw failure; }
        } catch (SQLException failure) { throw new IllegalStateException("SQL recheck unconfirmed; retry same action ID", failure); }
    }
}

package event.delivery.ingress.dlt;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import tools.jackson.databind.json.JsonMapper;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

/** SQL archive/cursor transaction plus per-record operation lock. Never commits a Kafka group. */
public final class DltIntakeStore {
    public record Scope(String cluster, String topic, int partition, String sourceTopic, String targetTopic) {
        public Scope {
            if (cluster == null || !cluster.matches("[A-Za-z0-9._-]{1,100}") || partition < 0)
                throw new IllegalArgumentException("Invalid intake scope");
            for (String name : Arrays.asList(topic, sourceTopic, targetTopic))
                if (name == null || !name.matches("[A-Za-z0-9._-]{1,249}") || name.equals(".") || name.equals(".."))
                    throw new IllegalArgumentException("Invalid topic");
            if (topic.equals(sourceTopic) || topic.equals(targetTopic)) throw new IllegalArgumentException("DLT must be a separate topic");
        }
    }
    public record Raw(String topic, int partition, long offset, byte[] key, byte[] value,
                      List<DltRecoveryCheckpoint.Header> headers) {
        static Raw capture(ConsumerRecord<byte[], byte[]> record) {
            var headers = new ArrayList<DltRecoveryCheckpoint.Header>();
            record.headers().forEach(h -> headers.add(new DltRecoveryCheckpoint.Header(h.key(), h.value())));
            return new Raw(record.topic(), record.partition(), record.offset(), record.key(), record.value(), List.copyOf(headers));
        }
        ConsumerRecord<byte[], byte[]> record() {
            var record = new ConsumerRecord<byte[], byte[]>(topic, partition, offset, key, value);
            headers.forEach(h -> record.headers().add(h.name(), h.value()));
            return record;
        }
    }
    public record Outcome(String state, String decision, UUID operationId) { }
    public record Backlog(long unprocessed, long held, Instant oldestHeldAt) { }
    private final DltRecoveryStore.Connections connections;
    private final String schema;
    private final JsonMapper mapper = JsonMapper.builder().build();
    public DltIntakeStore(DltRecoveryStore.Connections connections, String schema) {
        if (schema == null || !schema.matches("[a-z][a-z0-9_]{0,62}")) throw new IllegalArgumentException("Invalid schema");
        this.connections = connections; this.schema = schema;
    }
    private static void scope(PreparedStatement statement, Scope scope) throws SQLException {
        statement.setString(1, scope.cluster()); statement.setString(2, scope.topic()); statement.setInt(3, scope.partition());
    }
    public long cursor(Scope scope, long initialOffset) {
        if (initialOffset < 0) throw new IllegalArgumentException("Explicit initial offset required");
        try (var connection = connections.open(); var insert = connection.prepareStatement("INSERT INTO " + schema
                + ".dlt_intake_cursor(cluster_alias,topic,partition_id,source_topic,target_topic,next_offset) VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING")) {
            scope(insert, scope); insert.setString(4, scope.sourceTopic()); insert.setString(5, scope.targetTopic()); insert.setLong(6, initialOffset);
            insert.executeUpdate();
            try (var query = connection.prepareStatement("SELECT source_topic,target_topic,next_offset FROM " + schema
                    + ".dlt_intake_cursor WHERE cluster_alias=? AND topic=? AND partition_id=?")) {
                scope(query, scope);
                try (var row = query.executeQuery()) {
                    if (!row.next() || !scope.sourceTopic().equals(row.getString(1)) || !scope.targetTopic().equals(row.getString(2)))
                        throw new IllegalStateException("Intake scope changed");
                    return row.getLong(3);
                }
            }
        } catch (SQLException failure) { throw new IllegalStateException("SQL intake cursor unavailable", failure); }
    }
    public boolean archive(Scope scope, long expectedOffset, DltInspector.RawPage page) {
        if (page.beginningOffset() > expectedOffset || page.nextOffset() < expectedOffset
                || page.nextOffset() > page.snapshotEndOffset()) throw new IllegalArgumentException("Invalid archive range");
        try (var connection = connections.open()) {
            connection.setAutoCommit(false);
            try {
                try (var query = connection.prepareStatement("SELECT next_offset FROM " + schema
                        + ".dlt_intake_cursor WHERE cluster_alias=? AND topic=? AND partition_id=? FOR UPDATE")) {
                    scope(query, scope);
                    try (var row = query.executeQuery()) {
                        if (!row.next() || row.getLong(1) != expectedOffset) { connection.rollback(); return false; }
                    }
                }
                long previous = expectedOffset - 1;
                for (var record : page.records()) {
                    if (!scope.topic().equals(record.topic()) || scope.partition() != record.partition()
                            || record.offset() <= previous || record.offset() >= page.nextOffset()) throw new IllegalArgumentException("Invalid archive record");
                    previous = record.offset();
                    UUID id = UUID.nameUUIDFromBytes(("dlt-intake:" + scope.cluster() + ":" + record.topic() + ":"
                            + record.partition() + ":" + record.offset()).getBytes(StandardCharsets.UTF_8));
                    try (var insert = connection.prepareStatement("INSERT INTO " + schema
                            + ".dlt_intake_record(intake_id,cluster_alias,topic,partition_id,record_offset,record_json) VALUES (?,?,?,?,?,?)")) {
                        insert.setObject(1, id); insert.setString(2, scope.cluster()); insert.setString(3, scope.topic());
                        insert.setInt(4, scope.partition()); insert.setLong(5, record.offset()); insert.setString(6, mapper.writeValueAsString(Raw.capture(record)));
                        insert.executeUpdate();
                    }
                }
                try (var update = connection.prepareStatement("UPDATE " + schema
                        + ".dlt_intake_cursor SET next_offset=?,updated_at=clock_timestamp() WHERE cluster_alias=? AND topic=? AND partition_id=?")) {
                    update.setLong(1, page.nextOffset()); update.setString(2, scope.cluster()); update.setString(3, scope.topic()); update.setInt(4, scope.partition());
                    update.executeUpdate();
                }
                connection.commit(); return true;
            } catch (SQLException | RuntimeException failure) { connection.rollback(); throw failure; }
        } catch (SQLException failure) { throw new IllegalStateException("SQL intake archive unavailable", failure); }
    }
    public List<UUID> pending(Scope scope, int limit) {
        if (limit < 1 || limit > 100) throw new IllegalArgumentException("Limit 1..100 required");
        try (var connection = connections.open(); var query = connection.prepareStatement("SELECT intake_id FROM " + schema
                + ".dlt_intake_record WHERE cluster_alias=? AND topic=? AND partition_id=? AND state='NEW' ORDER BY updated_at,intake_id LIMIT ?")) {
            scope(query, scope); query.setInt(4, limit);
            var ids = new ArrayList<UUID>();
            try (var rows = query.executeQuery()) { while (rows.next()) ids.add(rows.getObject(1, UUID.class)); }
            return List.copyOf(ids);
        } catch (SQLException failure) { throw new IllegalStateException("SQL intake scan unavailable", failure); }
    }
    public Outcome process(UUID id, Function<ConsumerRecord<byte[], byte[]>, Outcome> action) {
        try (var connection = connections.open()) {
            try (var lock = connection.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
                lock.setLong(1, id.getMostSignificantBits());
                try (var row = lock.executeQuery()) { row.next(); if (!row.getBoolean(1)) return new Outcome("NEW", "BUSY", null); }
            }
            try {
                String json;
                try (var query = connection.prepareStatement("SELECT record_json,state,decision,operation_id FROM " + schema + ".dlt_intake_record WHERE intake_id=?")) {
                    query.setObject(1, id);
                    try (var row = query.executeQuery()) {
                        if (!row.next()) throw new IllegalStateException("Intake disappeared");
                        if (!"NEW".equals(row.getString(2))) return new Outcome(row.getString(2), row.getString(3), row.getObject(4, UUID.class));
                        json = row.getString(1);
                    }
                }
                Outcome outcome;
                try { outcome = action.apply(mapper.readValue(json, Raw.class).record()); }
                catch (RuntimeException failure) { outcome = new Outcome("NEW", "UNCONFIRMED", null); }
                if (!Set.of("NEW", "REGISTERED", "HELD").contains(outcome.state())) throw new IllegalArgumentException("Invalid intake outcome");
                try (var update = connection.prepareStatement("UPDATE " + schema
                        + ".dlt_intake_record SET state=?,decision=?,operation_id=?,updated_at=clock_timestamp() WHERE intake_id=?")) {
                    update.setString(1, outcome.state()); update.setString(2, outcome.decision()); update.setObject(3, outcome.operationId()); update.setObject(4, id);
                    update.executeUpdate();
                }
                return outcome;
            } finally {
                try (var unlock = connection.prepareStatement("SELECT pg_advisory_unlock(?)")) { unlock.setLong(1, id.getMostSignificantBits()); unlock.execute(); }
            }
        } catch (SQLException failure) { throw new IllegalStateException("SQL intake processing unavailable", failure); }
    }
    public Backlog backlog(Scope scope) {
        try (var connection = connections.open(); var query = connection.prepareStatement("SELECT count(*) FILTER (WHERE state='NEW'),"
                + "count(*) FILTER (WHERE state='HELD'),min(created_at) FILTER (WHERE state='HELD') FROM " + schema
                + ".dlt_intake_record WHERE cluster_alias=? AND topic=? AND partition_id=? AND state IN ('NEW','HELD')")) {
            scope(query, scope);
            try (var row = query.executeQuery()) {
                row.next(); var oldest = row.getTimestamp(3);
                return new Backlog(row.getLong(1), row.getLong(2), oldest == null ? null : oldest.toInstant());
            }
        } catch (SQLException failure) { throw new IllegalStateException("SQL intake backlog unavailable", failure); }
    }
}

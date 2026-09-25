package event.delivery.ingress.dlt;

import event.common.delivery.*;
import event.common.lifecycle.DeliveryCompletion;
import event.delivery.ingress.repository.DeliveryRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import tools.jackson.databind.json.JsonMapper;
import java.time.*;
import java.util.*;

/** Absent ORIGIN requires retained SQL history coverage plus a fenced reservation before activation. */
public final class DltRecoveryPlanner {
    public enum Decision { INVALID_DLT, NO_ORIGIN_UNCONFIRMED, HISTORY_FOUND, ORIGIN_MISMATCH,
        COMPLETION_RECORDED, STEP_ALREADY_TRACKED, RESUME_EXISTING, EXPIRE_EXISTING, RESTORE_ORIGIN, RECOVERY_HELD }
    public record Preview(DltInspection.Assessment dlt, Decision decision, String executionId,
                          boolean historyExists, boolean stepExists) { }
    public record Plan(Preview preview, DeliveryEvent command) {
        public boolean eligible() { return command != null; }
    }
    public record History(boolean anyRequestHistory, boolean executionHistory, Instant completeSince) {
        public boolean covers(Instant received) { return completeSince != null && !received.isBefore(completeSince); }
    }
    @FunctionalInterface public interface HistoryReader { History read(long tenant, String key, String execution); }
    private final DynamoDbClient db;
    private final HistoryReader history;
    private final DltInspection inspection;
    private final Clock clock;
    private final JsonMapper mapper = JsonMapper.builder().build();

    public DltRecoveryPlanner(DynamoDbClient db, HistoryReader history, Duration primaryTtl, String sourceTopic, Clock clock) {
        this.db = db; this.history = history; this.inspection = new DltInspection(sourceTopic, primaryTtl); this.clock = clock;
    }

    public Plan plan(ConsumerRecord<byte[], byte[]> record) {
        var assessment = inspection.assess(record, clock.instant());
        if (assessment.decision() != DltInspection.Decision.REQUIRES_STATE_CHECK
                && assessment.decision() != DltInspection.Decision.EXPIRED_REQUIRES_FINALIZATION) {
            return new Plan(new Preview(assessment, Decision.INVALID_DLT, null, false, false), null);
        }
        var event = mapper.readValue(record.value(), DeliveryEvent.class);
        var origin = db.getItem(r -> r.tableName("ORIGIN").key(Map.of("pk", s("DELIVERY#" + event.requestKey()), "sk", s("META")))
                .consistentRead(true)).item();
        String execution = origin.isEmpty() ? null : value(origin, "delivery_id");
        // SQL errors must propagate; unavailable history is never treated as absent history.
        var past = history.read(event.tenantId(), event.requestKey(), execution);
        String recoveryId = recoveryId(assessment);
        if (origin.isEmpty()) {
            if (past.anyRequestHistory() || !past.covers(event.occurredAt())) {
                return held(assessment, past.anyRequestHistory() ? Decision.HISTORY_FOUND : Decision.NO_ORIGIN_UNCONFIRMED,
                        execution, past, false);
            }
            return restore(assessment, event, recoveryId, past);
        }
        if (origin.containsKey(DeliveryCompletion.RECOVERY_HOLD)) {
            if (!recoveryId.equals(value(origin, DeliveryCompletion.RECOVERY_HOLD))
                    || !recoveryId.equals(execution) || !matches(event, origin)) {
                return held(assessment, Decision.RECOVERY_HELD, execution, past, false);
            }
            if (past.anyRequestHistory() || !past.covers(event.occurredAt())) {
                return held(assessment, past.anyRequestHistory() ? Decision.HISTORY_FOUND : Decision.RECOVERY_HELD,
                        execution, past, false);
            }
            return restore(assessment, event, recoveryId, past);
        }
        if (past.executionHistory()) return held(assessment, Decision.HISTORY_FOUND, execution, past, false);
        if (origin.containsKey("completion_event_id") || origin.containsKey("completion_fence")) {
            return held(assessment, Decision.COMPLETION_RECORDED, execution, past, false);
        }
        if (!matches(event, origin)) return held(assessment, Decision.ORIGIN_MISMATCH, execution, past, false);
        var steps = db.query(r -> r.tableName("STEP").keyConditionExpression("pk = :pk")
                .expressionAttributeValues(Map.of(":pk", s("DELIVERY#" + execution))).consistentRead(true).limit(1)).items();
        if (!steps.isEmpty()) return held(assessment, Decision.STEP_ALREADY_TRACKED, execution, past, true);
        Decision decision = assessment.decision() == DltInspection.Decision.EXPIRED_REQUIRES_FINALIZATION
                ? Decision.EXPIRE_EXISTING : Decision.RESUME_EXISTING;
        return new Plan(new Preview(assessment, decision, execution, past.anyRequestHistory(), false),
                event.execution(execution).toDispatchRequested());
    }

    /** Called only after the SQL operation and STARTED attempt are committed by DltRecoveryStore. */
    public Plan prepare(ConsumerRecord<byte[], byte[]> record) {
        return prepare(record, null);
    }

    public Plan prepare(ConsumerRecord<byte[], byte[]> record, DeliveryEvent expectedCommand) {
        var before = plan(record);
        // A persisted operation must never activate a replacement generation before its identity is compared.
        if (expectedCommand != null && !expectedCommand.equals(before.command())) return before;
        if (before.preview().decision() != Decision.RESTORE_ORIGIN) return before;
        var admission = mapper.readValue(record.value(), DeliveryEvent.class);
        String id = recoveryId(before.preview().dlt());
        var execution = admission.execution(id);
        var repository = new DeliveryRepository(db, mapper);
        try { repository.reserveRecovery(execution, id); }
        catch (ConditionalCheckFailedException occupied) { /* Recheck the actual owner below. */ }
        // SQL is read again after reserving ORIGIN. Prior cleanup must commit history before freeing this key.
        var after = plan(record);
        if (after.preview().decision() != Decision.RESTORE_ORIGIN || !before.command().equals(after.command())) {
            repository.discardRecovery(execution, id);
            return after;
        }
        repository.activateRecovery(execution, id);
        return plan(record);
    }

    private Plan restore(DltInspection.Assessment assessment, DeliveryEvent event, String id, History history) {
        return new Plan(new Preview(assessment, Decision.RESTORE_ORIGIN, id, history.anyRequestHistory(), false),
                event.execution(id).toDispatchRequested());
    }

    private static String recoveryId(DltInspection.Assessment assessment) {
        var source = assessment.source();
        String identity = "dlt-origin:v1:" + assessment.tenantId() + ":" + assessment.requestKey() + ":"
                + source.topic() + ":" + source.partition() + ":" + source.offset() + ":" + assessment.valueSha256();
        return UUID.nameUUIDFromBytes(identity.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();
    }

    private boolean matches(DeliveryEvent event, Map<String, AttributeValue> origin) {
        try {
            String execution = value(origin, "delivery_id");
            UUID.fromString(execution);
            return value(origin, "schema_version").equals("2")
                    && event.requestKey().equals(value(origin, "request_key"))
                    && DeliveryIds.eventId(execution, DeliveryEventType.DELIVERY_REQUESTED).equals(value(origin, "event_id"))
                    && event.tenantId().toString().equals(value(origin, "tenant_id"))
                    && event.deliveryType().equals(value(origin, "delivery_type"))
                    && event.occurredAt().equals(Instant.parse(value(origin, "occurred_at")))
                    && event.fallbackAllowed().equals(origin.getOrDefault("fallback_allowed", AttributeValue.fromBool(false)).bool())
                    && mapper.writeValueAsString(DeliveryPayloads.canonicalize(event.payload())).equals(value(origin, "payload"));
        } catch (RuntimeException invalid) { return false; }
    }
    private static Plan held(DltInspection.Assessment dlt, Decision decision, String execution, History history, boolean step) {
        return new Plan(new Preview(dlt, decision, execution, history.anyRequestHistory(), step), null);
    }
    private static AttributeValue s(String text) { return AttributeValue.fromS(text); }
    private static String value(Map<String, AttributeValue> map, String name) {
        var value = map.get(name); return value == null ? "" : value.s() == null ? value.n() : value.s();
    }
}

package event.delivery.ingress.dlt;

import event.common.delivery.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import tools.jackson.databind.json.JsonMapper;
import java.time.*;
import java.util.*;

/** A current ORIGIN is required: never recreate an execution from a possibly old DLT. */
public final class DltRecoveryPlanner {
    public enum Decision { INVALID_DLT, NO_ORIGIN_UNCONFIRMED, HISTORY_FOUND, ORIGIN_MISMATCH,
        COMPLETION_RECORDED, STEP_ALREADY_TRACKED, RESUME_EXISTING, EXPIRE_EXISTING }
    public record Preview(DltInspection.Assessment dlt, Decision decision, String executionId,
                          boolean historyExists, boolean stepExists) { }
    public record Plan(Preview preview, DeliveryEvent command) {
        public boolean eligible() { return command != null; }
    }
    public record History(boolean anyRequestHistory, boolean executionHistory) { }
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
        if (origin.isEmpty()) return held(assessment, past.anyRequestHistory() ? Decision.HISTORY_FOUND : Decision.NO_ORIGIN_UNCONFIRMED,
                execution, past, false);
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

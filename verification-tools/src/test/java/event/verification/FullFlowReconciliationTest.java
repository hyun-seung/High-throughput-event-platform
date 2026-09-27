package event.verification;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FullFlowReconciliationTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String EVIDENCE = """
            {"starts":{"0":{"started":1000}},
             "responses":{"0":{"deliveryId":"request-1","status":202}},
             "history":[{"result_json":{"deliveryId":"execution-1","requestKey":"request-1",
                "outcome":"DELIVERED","routeOrder":1,"resultAt":"1970-01-01T00:00:02Z",
                "finalizedAt":"1970-01-01T00:00:03Z"},"notification_status":"DELIVERED",
                "cleanup_status":"DONE","stored_at":"1970-01-01T00:00:04Z",
                "cleanup_completed_at":"1970-01-01T00:00:06Z"}],
             "callbacks":[{"status":204,"receivedAt":"1970-01-01T00:00:05Z",
               "body":{"results":[{"deliveryId":"execution-1","requestKey":"request-1",
                 "outcome":"DELIVERED","routeOrder":1,"resultAt":"1970-01-01T00:00:02Z",
                 "finalizedAt":"1970-01-01T00:00:03Z"}]}}]}
            """;

    @Test
    void measuresFiveStagesFromClientStart() {
        Map<String, Object> report = reconcile(EVIDENCE);
        @SuppressWarnings("unchecked") var stats = (Map<String, Map<String, Double>>) report.get("stats");
        assertEquals(1000, stats.get("providerResult").get("p95"));
        assertEquals(2000, stats.get("finalized").get("p95"));
        assertEquals(3000, stats.get("sqlStored").get("p95"));
        assertEquals(4000, stats.get("customerReceived").get("p95"));
        assertEquals(5000, stats.get("cleanup").get("p95"));
    }

    @Test
    void rejectsMismatchedOrDuplicateHistory() {
        rejects(EVIDENCE.replaceFirst("request-1", "other"));
        rejects(EVIDENCE.replace("\"cleanup_completed_at\":\"1970-01-01T00:00:06Z\"}]",
                "\"cleanup_completed_at\":\"1970-01-01T00:00:06Z\"},{\"result_json\":{\"requestKey\":\"request-1\"}}]"));
    }

    @Test
    void rejectsMissingOrConflictingCustomerReceipt() {
        rejects(EVIDENCE.substring(0, EVIDENCE.indexOf("\"callbacks\"")) + "\"callbacks\":[]}");
        String outcome = "\"outcome\":\"DELIVERED\"";
        int callbackOutcome = EVIDENCE.lastIndexOf(outcome);
        rejects(EVIDENCE.substring(0, callbackOutcome) + "\"outcome\":\"EXPIRED\""
                + EVIDENCE.substring(callbackOutcome + outcome.length()));
    }

    @Test
    void rejectsIncompleteWorkflowUnconfirmedInputAndNegativeLatency() {
        rejects(EVIDENCE.replace("\"cleanup_status\":\"DONE\"", "\"cleanup_status\":\"PENDING\""));
        rejects(EVIDENCE.replace("\"status\":202", "\"status\":503"));
        rejects(EVIDENCE.replace("\"started\":1000", "\"started\":7000"));
    }

    private static Map<String, Object> reconcile(String json) {
        return FullFlowReconciliation.reconcile(JSON.readTree(json));
    }

    private static void rejects(String json) {
        assertThrows(IllegalArgumentException.class, () -> reconcile(json));
    }
}

package event.verification;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PocReconciliationTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String DELIVERY = "89531340-94c6-3d4b-8723-db0b64183606";
    private static final String ATTEMPT = "bd87ee93-7305-3ab8-af8f-38e3bc7de582";

    @Test
    void acceptsOneConfirmedAndPersistedRequest() {
        Map<String, Object> report = reconcile(fixture("{\"calls\":1,\"effects\":1}"));
        @SuppressWarnings("unchecked") var summary = (Map<String, Object>) report.get("summary");
        assertTrue((Boolean) summary.get("consistent"));
        @SuppressWarnings("unchecked") var rows = (List<Map<String, Object>>) report.get("rows");
        assertEquals(DELIVERY, rows.getFirst().get("deliveryId"));
        assertEquals(100d, (Double) rows.getFirst().get("persistedTimestampLatencyMs"));
    }

    @Test
    void missingProviderEffectAndExtraKafkaRecordCannotPass() {
        Map<String, Object> missing = reconcile(fixture("{}"));
        @SuppressWarnings("unchecked") var rows = (List<Map<String, Object>>) missing.get("rows");
        assertTrue(((List<?>) rows.getFirst().get("problems")).contains("provider_call_or_effect_mismatch"));
        String extra = fixture("{\"calls\":1,\"effects\":1}").replace("\"records\":[",
                "\"records\":[{\"topic\":\"delivery.requested.v1\",\"deliveryId\":\"other\"},");
        @SuppressWarnings("unchecked") var summary = (Map<String, Object>) reconcile(extra).get("summary");
        assertFalse((Boolean) summary.get("consistent"));
        assertEquals(List.of("other"), summary.get("unexpectedKafkaIds"));
    }

    @Test
    void rejectsUnparseableKafkaEvidence() {
        String invalid = fixture("{\"calls\":1,\"effects\":1}").replace("\"deliveryId\":\"" + DELIVERY + "\"",
                "\"deliveryId\":null");
        assertThrows(IllegalArgumentException.class, () -> reconcile(invalid));
    }

    private static Map<String, Object> reconcile(String input) {
        return PocReconciliation.reconcile(JSON.readTree(input));
    }

    private static String fixture(String counts) {
        return """
                {"starts":{"0":{"key":"test-key"}},"results":{"0":{"status":202,"deliveryId":"%s"}},
                 "tenant":1,"records":[{"topic":"delivery.requested.v1","deliveryId":"%s"},
                     {"topic":"delivery.dispatch-requested.v1","deliveryId":"%s"}],
                 "items":[{"pk":"DELIVERY#%s","sk":"META",
                     "item":{"occurred_at":{"S":"2026-09-24T00:00:00Z"}}},
                     {"pk":"DELIVERY#%s","sk":"ATTEMPT#%s",
                      "item":{"status":{"S":"ACCEPTED"},"updated_at":{"S":"2026-09-24T00:00:00.100Z"}}}],
                 "provider":{"%s":%s}}
                """.formatted(DELIVERY, DELIVERY, DELIVERY, DELIVERY, DELIVERY, ATTEMPT, ATTEMPT, counts);
    }
}

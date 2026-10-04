package messaging.verification;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaOutageEvidenceTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String RESULT = """
            {"deliveryId":"execution","requestKey":"request","outcome":"EXPIRED","routeOrder":1,
             "deadline":"2026-09-25T00:00:00Z","resultAt":"2026-09-25T00:00:00Z"}
            """.strip();
    private static final String EVIDENCE = """
            {"kafka":{"status":"exited","running":false},"requestKey":"request",
             "publishFailuresBefore":0,"publishFailuresAfter":1,"historyRowsBefore":2,"historyRows":2,
             "customerRequestsBefore":2,"customerRequests":2,"completedRedisTtl":-2,
             "origin":{"delivery_id":{"S":"execution"}},
             "step":{"result_event":{"S":%s},
                     "deadline_at":{"N":"1790294400000"}},
             "provider":[{"calls":1,"effects":1},{"calls":0,"effects":0}]}
            """.formatted(JSON.writeValueAsString(RESULT));

    @Test void acceptsRetainedExpiry() {
        assertDoesNotThrow(() -> KafkaOutageEvidence.verify(JSON.readTree(EVIDENCE)));
    }

    @Test void rejectsEarlyHandoffOrCleanup() {
        for (String changed : new String[]{
                EVIDENCE.replace("\"status\":\"exited\"", "\"status\":\"running\""),
                EVIDENCE.replace("\"publishFailuresAfter\":1", "\"publishFailuresAfter\":0"),
                EVIDENCE.replace("\"historyRows\":2", "\"historyRows\":3"),
                EVIDENCE.replace("\"customerRequests\":2", "\"customerRequests\":3"),
                EVIDENCE.replace("\"completedRedisTtl\":-2", "\"completedRedisTtl\":86400"),
                EVIDENCE.replace("\"calls\":1", "\"calls\":2"),
                EVIDENCE.replace("\"historyRows\":2,", ""),
                EVIDENCE.replace("{\"calls\":0,\"effects\":0}", "{\"calls\":0}")})
            assertThrows(IllegalArgumentException.class, () -> KafkaOutageEvidence.verify(JSON.readTree(changed)));
    }

    @Test void rejectsWrongIdentityOutcomeAndDeadline() {
        for (String changed : new String[]{
                EVIDENCE.replace("\\\"deliveryId\\\":\\\"execution\\\"", "\\\"deliveryId\\\":\\\"other\\\""),
                EVIDENCE.replace("\\\"requestKey\\\":\\\"request\\\"", "\\\"requestKey\\\":\\\"other\\\""),
                EVIDENCE.replace("\\\"outcome\\\":\\\"EXPIRED\\\"", "\\\"outcome\\\":\\\"DELIVERED\\\""),
                EVIDENCE.replace("\\\"resultAt\\\":\\\"2026-09-25T00:00:00Z\\\"", "\\\"resultAt\\\":\\\"2026-09-25T00:00:01Z\\\""),
                EVIDENCE.replace("1790294400000", "1790294400001")})
            assertThrows(IllegalArgumentException.class, () -> KafkaOutageEvidence.verify(JSON.readTree(changed)));
    }
}

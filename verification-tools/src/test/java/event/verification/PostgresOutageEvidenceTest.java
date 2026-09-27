package event.verification;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PostgresOutageEvidenceTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String EVIDENCE = """
            {"before":{"applicationPids":{"result":101},"storeFailures":0,
                       "offsets":[{"partition":0,"committed":-1001,"high":0,"lag":0}]},
             "during":{"postgres":{"status":"exited","running":false},"sqlProbeRejected":true,
                       "applicationPids":{"result":101},"storeFailures":2,"customerRequests":0,
                       "offsets":[{"partition":0,"committed":-1001,"high":2,"lag":2}],
                       "requests":[%s,%s]}}
            """.formatted(row("DELIVERED"), row("EXPIRED"));

    @Test void acceptsDurableResultsWithUncommittedKafkaOffsets() {
        assertDoesNotThrow(() -> verify(EVIDENCE));
    }

    @Test void rejectsAvailableSqlOrLostKafkaWork() {
        for (String changed : new String[]{
                EVIDENCE.replace("\"status\":\"exited\"", "\"status\":\"running\""),
                EVIDENCE.replace("\"sqlProbeRejected\":true", "\"sqlProbeRejected\":false"),
                EVIDENCE.replace("\"storeFailures\":2", "\"storeFailures\":0"),
                EVIDENCE.replace("\"customerRequests\":0", "\"customerRequests\":1"),
                EVIDENCE.replace("\"committed\":-1001,\"high\":2", "\"committed\":1,\"high\":2"),
                EVIDENCE.replace("\"lag\":2", "\"lag\":1"),
                EVIDENCE.replace("\"applicationPids\":{\"result\":101},\"storeFailures\":2",
                        "\"applicationPids\":{\"result\":102},\"storeFailures\":2")})
            assertThrows(IllegalArgumentException.class, () -> verify(changed));
    }

    @Test void rejectsWrongResultIdentityDeadlineAndProviderEffects() {
        for (String changed : new String[]{
                EVIDENCE.replace("\"origin\":{\"delivery_id\":{\"S\":\"DELIVERED\"}}",
                        "\"origin\":{\"delivery_id\":{\"S\":\"wrong\"}}"),
                EVIDENCE.replace("\"expectedOutcome\":\"DELIVERED\"", "\"expectedOutcome\":\"EXPIRED\""),
                EVIDENCE.replace("\"requestKey\":\"DELIVERED\",\"origin\"", "\"requestKey\":\"wrong\",\"origin\""),
                EVIDENCE.replace("\"completedRedisTtl\":-2", "\"completedRedisTtl\":30"),
                EVIDENCE.replace("\"calls\":1", "\"calls\":2"),
                EVIDENCE.replace("1790294400000", "1790294400001")})
            assertThrows(IllegalArgumentException.class, () -> verify(changed));
    }

    private static String row(String outcome) {
        String result = """
                {"deliveryId":"%s","requestKey":"%s","outcome":"%s","routeOrder":1,
                 "resultAt":"2026-09-25T00:00:00Z","deadline":"2026-09-25T00:00:00Z"}
                """.formatted(outcome, outcome, outcome).strip();
        return """
                {"expectedOutcome":"%s","requestKey":"%s","origin":{"delivery_id":{"S":"%s"}},
                 "step":{"result_event":{"S":%s},"deadline_at":{"N":"1790294400000"}},
                 "provider":[{"calls":1,"effects":1},{"calls":0,"effects":0}],"completedRedisTtl":-2}
                """.formatted(outcome, outcome, outcome, JSON.writeValueAsString(result)).strip();
    }

    private static void verify(String evidence) {
        PostgresOutageEvidence.verify(JSON.readTree(evidence));
    }
}

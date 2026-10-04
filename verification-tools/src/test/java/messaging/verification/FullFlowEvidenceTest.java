package messaging.verification;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FullFlowEvidenceTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String RESULT = """
            {"deliveryId":"execution-1","requestKey":"request-1","outcome":"DELIVERED","routeOrder":1}
            """.strip();
    private static final String EVIDENCE = """
            {"row":{"result_json":%s,"notification_status":"DELIVERED","cleanup_status":"DONE"},
             "outcome":"DELIVERED","route":1,"expectedCalls":[1,0],
             "provider":[{"calls":1},{"calls":0}],"origin":null,"steps":[],"completedTtl":86000,
             "callbacks":[{"status":503,"body":{"results":[%s]}},
                          {"status":204,"body":{"results":[%s]}}]}
            """.formatted(RESULT, RESULT, RESULT).replaceAll("\\s+", "");

    @Test void acceptsDurableResultAfterIdenticalCustomerRetry() {
        assertDoesNotThrow(() -> verify(EVIDENCE));
        String repeatedAck = EVIDENCE.replace("{\"status\":204,\"body\":{\"results\":[" + RESULT + "]}}]",
                "{\"status\":204,\"body\":{\"results\":[" + RESULT + "]}},{\"status\":204,\"body\":{\"results\":[" + RESULT + "]}}]");
        assertNotEquals(EVIDENCE, repeatedAck);
        assertDoesNotThrow(() -> verify(repeatedAck));
    }

    @Test void rejectsUnacknowledgedOrChangedCustomerResult() {
        for (String changed : new String[]{
                EVIDENCE.replace("\"status\":204", "\"status\":503"),
                EVIDENCE.replace("{\"status\":204,\"body\":{\"results\":[" + RESULT + "]}}",
                        "{\"status\":204,\"body\":{\"results\":[{\"deliveryId\":\"execution-1\",\"requestKey\":\"request-1\",\"outcome\":\"EXPIRED\",\"routeOrder\":1}]}}")})
            rejectChangedEvidence(changed);
    }

    @Test void rejectsUncompactedItemsProviderDriftAndMissingCompletion() {
        for (String changed : new String[]{
                EVIDENCE.replace("\"origin\":null", "\"origin\":{\"pk\":\"retained\"}"),
                EVIDENCE.replace("\"steps\":[]", "\"steps\":[{\"pk\":\"retained\"}]"),
                EVIDENCE.replace("\"provider\":[{\"calls\":1},{\"calls\":0}]", "\"provider\":[{\"calls\":1},{\"calls\":1}]"),
                EVIDENCE.replace("\"completedTtl\":86000", "\"completedTtl\":-2"),
                EVIDENCE.replace("\"cleanup_status\":\"DONE\"", "\"cleanup_status\":\"PENDING\""),
                EVIDENCE.replace("\"notification_status\":\"DELIVERED\"", "\"notification_status\":\"PENDING\"")})
            rejectChangedEvidence(changed);
    }

    private static void rejectChangedEvidence(String changed) {
        assertNotEquals(EVIDENCE, changed);
        assertThrows(IllegalArgumentException.class, () -> verify(changed));
    }

    private static void verify(String input) {
        FullFlowEvidence.verify(JSON.readTree(input));
    }
}

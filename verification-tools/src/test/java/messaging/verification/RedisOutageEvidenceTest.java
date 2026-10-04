package messaging.verification;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RedisOutageEvidenceTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String EVIDENCE = """
            {"row":{"result_json":{"deliveryId":"execution","outcome":"EXPIRED","routeOrder":1},
                    "notification_status":"DELIVERED","cleanup_status":"DONE"},
             "expected":"EXPIRED","provider":[{"calls":1,"effects":1},{"calls":0,"effects":0}],
             "origin":{},"steps":[],
             "callbacks":[{"status":204,"body":{"results":[{"deliveryId":"execution","outcome":"EXPIRED","routeOrder":1}]}}]}
            """;

    @Test void acceptsDurableCompletionWithoutRedisMarker() {
        assertDoesNotThrow(() -> RedisOutageEvidence.verify(JSON.readTree(EVIDENCE)));
    }

    @Test void rejectsMissingDeliveryCleanupOrUnexpectedProviderEffect() {
        for (String changed : new String[]{
                EVIDENCE.replace("\"notification_status\":\"DELIVERED\"", "\"notification_status\":\"PENDING\""),
                EVIDENCE.replace("\"cleanup_status\":\"DONE\"", "\"cleanup_status\":\"PENDING\""),
                EVIDENCE.replace("\"calls\":1", "\"calls\":2"),
                EVIDENCE.replace("\"origin\":{}", "\"origin\":{\"pk\":\"retained\"}"),
                EVIDENCE.replace("\"steps\":[]", "\"steps\":[{\"pk\":\"retained\"}]"),
                EVIDENCE.replace("\"status\":204", "\"status\":503")})
            assertThrows(IllegalArgumentException.class, () -> RedisOutageEvidence.verify(JSON.readTree(changed)));
    }

    @Test void rejectsWrongCustomerResultEvenWhenHttpAcknowledged() {
        String changed = EVIDENCE.replace("\"results\":[{\"deliveryId\":\"execution\",\"outcome\":\"EXPIRED\"",
                        "\"results\":[{\"deliveryId\":\"execution\",\"outcome\":\"DELIVERED\"");
        assertThrows(IllegalArgumentException.class, () -> RedisOutageEvidence.verify(JSON.readTree(changed)));
    }
}

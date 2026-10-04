package messaging.verification;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProcessRecoveryEvidenceTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String BODY = """
            {"batchId":"batch-1","results":[{"eventId":"event-1"}]}
            """.strip();
    private static final String ACK = """
            {"before":{"status":"IN_FLIGHT","attempt_count":1,"batch_id":"batch-1","request_body":%s},
             "after":{"status":"DELIVERED","attempt_count":2,"batch_id":"batch-1","request_body":%s},
             "callbacks":[{"status":0,"body":%s},{"status":204,"body":%s}]}
            """.formatted(JSON.writeValueAsString(BODY), JSON.writeValueAsString(BODY), BODY, BODY).replaceAll("\\s+", "");
    private static final String UNCERTAIN = """
            {"before":{"status":{"S":"PROCESSING"},"version":{"N":"1"},"retry_count":{"N":"0"},
                       "deadline_at":{"N":"90000"},"lease_until":{"N":"10000"}},
             "after":{"status":{"S":"PROCESSING"},"version":{"N":"1"},"retry_count":{"N":"0"},
                      "deadline_at":{"N":"90000"},"lease_until":{"N":"10000"}},
             "provider":{"calls":1,"effects":1}}
            """.replaceAll("\\s+", "");

    @Test void acceptsSameBatchAfterLostCustomerAck() {
        assertDoesNotThrow(() -> ProcessRecoveryEvidence.verifyAckLoss(JSON.readTree(ACK)));
    }

    @Test void rejectsChangedBatchMissingRetryOrEarlierAck() {
        for (String changed : new String[]{
                ACK.replace("\"status\":\"DELIVERED\"", "\"status\":\"PENDING\""),
                ACK.replace("\"attempt_count\":2", "\"attempt_count\":3"),
                ACK.replace("\"status\":0", "\"status\":204"),
                ACK.replace("\"after\":{\"status\":\"DELIVERED\",\"attempt_count\":2,\"batch_id\":\"batch-1\"",
                        "\"after\":{\"status\":\"DELIVERED\",\"attempt_count\":2,\"batch_id\":\"other\""),
                ACK.replace("\"status\":204,\"body\":", "\"status\":503,\"body\":"),
                ACK.replace("\"status\":204,\"body\":{\"batchId\":\"batch-1\",\"results\":[{\"eventId\":\"event-1\"}]}",
                        "\"status\":204,\"body\":{\"batchId\":\"batch-1\",\"results\":[{\"eventId\":\"other\"}]}")})
            assertThrows(IllegalArgumentException.class, () -> ProcessRecoveryEvidence.verifyAckLoss(JSON.readTree(changed)));
    }

    @Test void acceptsPreservedUncertainStepWithOneProviderEffect() {
        assertDoesNotThrow(() -> ProcessRecoveryEvidence.verifyUncertainStep(JSON.readTree(UNCERTAIN)));
    }

    @Test void rejectsAdvancedAttemptDeadlineOrDuplicateProviderEffect() {
        for (String changed : new String[]{
                UNCERTAIN.replace("\"deadline_at\":{\"N\":\"90000\"},\"lease_until\":{\"N\":\"10000\"}},\"provider\"",
                        "\"deadline_at\":{\"N\":\"90001\"},\"lease_until\":{\"N\":\"10000\"}},\"provider\""),
                UNCERTAIN.replace("\"provider\":{\"calls\":1", "\"provider\":{\"calls\":2"),
                UNCERTAIN.replace("\"provider\":{\"calls\":1,\"effects\":1}", "\"provider\":{\"calls\":1,\"effects\":0}"),
                UNCERTAIN.replace("\"after\":{\"status\":{\"S\":\"PROCESSING\"}", "\"after\":{\"status\":{\"S\":\"RETRY\"}")})
            assertThrows(IllegalArgumentException.class, () -> ProcessRecoveryEvidence.verifyUncertainStep(JSON.readTree(changed)));
    }
}

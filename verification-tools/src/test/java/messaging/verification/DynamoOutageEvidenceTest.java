package messaging.verification;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DynamoOutageEvidenceTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String CLAIM = """
            {"before":{"applicationPids":{"dispatch":123},"metrics":{"claimFailed":0,"lifecycleFailed":0},
                       "dispatchOffsets":[{"partition":0,"committed":-1001,"lag":1}]},
             "during":{"dynamo":{"status":"exited","running":false},"applicationPids":{"dispatch":123},
                       "metrics":{"claimFailed":2,"lifecycleFailed":1},
                       "dispatchOffsets":[{"partition":0,"committed":-1001,"lag":1}],
                       "provider":[{"calls":0,"effects":0},{"calls":0,"effects":0}],
                       "historyRows":0,"customerResults":0,"completedRedisTtl":-2},
             "calls":0,"failure":"claimFailed"}
            """.replaceAll("\\s+", "");

    @Test void distinguishesPreClaimBlockFromAcceptedAttemptExpiry() {
        assertDoesNotThrow(() -> verify(CLAIM));
        String expiry = CLAIM.replace("\"provider\":[{\"calls\":0,\"effects\":0}",
                        "\"provider\":[{\"calls\":1,\"effects\":1}")
                .replace("\"calls\":0,\"failure\":\"claimFailed\"", "\"calls\":1,\"failure\":\"lifecycleFailed\"");
        assertDoesNotThrow(() -> verify(expiry));
    }

    @Test void rejectsFalseOutageOffsetAdvanceAndEarlyEffects() {
        for (String changed : new String[]{
                CLAIM.replace("\"status\":\"exited\"", "\"status\":\"running\""),
                CLAIM.replace("\"applicationPids\":{\"dispatch\":123},\"metrics\":{\"claimFailed\":2",
                        "\"applicationPids\":{\"dispatch\":124},\"metrics\":{\"claimFailed\":2"),
                CLAIM.replace("\"claimFailed\":2", "\"claimFailed\":0"),
                CLAIM.replace("\"historyRows\":0", "\"historyRows\":1"),
                CLAIM.replace("\"customerResults\":0", "\"customerResults\":1"),
                CLAIM.replace("\"completedRedisTtl\":-2", "\"completedRedisTtl\":24"),
                CLAIM.replace("\"committed\":-1001,\"lag\":1}],\"provider\"",
                        "\"committed\":1,\"lag\":1}],\"provider\""),
                CLAIM.replace("\"provider\":[{\"calls\":0,\"effects\":0}",
                        "\"provider\":[{\"calls\":1,\"effects\":1}"),
                CLAIM.replace("\"lag\":1}],\"provider\"", "\"lag\":0}],\"provider\"")})
            assertThrows(IllegalArgumentException.class, () -> verify(changed));
    }

    private static void verify(String input) {
        DynamoOutageEvidence.verify(JSON.readTree(input));
    }
}

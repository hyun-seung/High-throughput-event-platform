package event.verification;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LifecycleEvidenceTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String RESULT = "{\"deliveryId\":\"delivery-1\",\"outcome\":\"DELIVERED\",\"routeOrder\":1}";
    private static final String FINAL = """
            {"kind":"final","outbox":{"publish_state":{"S":"PUBLISHED"},"result_event":{"S":"%s"}},
             "result":%s,"outcome":"DELIVERED","route":1,"primaryCalls":1,"secondaryCalls":0,
             "primaryProvider":{"calls":1},"secondaryProvider":null}
            """.formatted(RESULT.replace("\"", "\\\""), RESULT).replaceAll("\\s+", "");
    private static final String KAFKA = """
            {"kind":"kafka","expected":{"delivery-1":%s},"records":[%s],"endOffset":1}
            """.formatted(RESULT, RESULT).replaceAll("\\s+", "");

    @Test void acceptsPublishedOutboxAndOneMatchingKafkaRecord() {
        assertDoesNotThrow(() -> verify(FINAL));
        assertDoesNotThrow(() -> verify(KAFKA));
    }

    @Test void rejectsWrongOutboxResultProviderAndRetainedIndex() {
        for (String changed : new String[]{
                FINAL.replace("\"outcome\":\"DELIVERED\",\"route\"", "\"outcome\":\"EXPIRED\",\"route\""),
                FINAL.replace("\"primaryProvider\":{\"calls\":1}", "\"primaryProvider\":{\"calls\":2}"),
                FINAL.replace("\"secondaryProvider\":null", "\"secondaryProvider\":{\"calls\":1}"),
                FINAL.replace("\"publish_state\":{\"S\":\"PUBLISHED\"}", "\"publish_state\":{\"S\":\"PENDING\"}"),
                FINAL.replace("\\\"deliveryId\\\":\\\"delivery-1\\\"", "\\\"deliveryId\\\":\\\"other-delivery\\\""),
                FINAL.replace("\"outbox\":{", "\"outbox\":{\"lifecycle_bucket\":{\"S\":\"lifecycle-v1-0\"},")})
            rejectChanged(FINAL, changed);
    }

    @Test void rejectsMissingDuplicateAndChangedKafkaRecords() {
        for (String changed : new String[]{
                KAFKA.replace("\"records\":[" + RESULT + "]", "\"records\":[]"),
                KAFKA.replace("\"records\":[" + RESULT + "]", "\"records\":[" + RESULT + "," + RESULT + "]"),
                KAFKA.replace("\"endOffset\":1", "\"endOffset\":2"),
                KAFKA.replace("\"records\":[" + RESULT + "]", "\"records\":[{\"deliveryId\":\"delivery-1\",\"outcome\":\"EXPIRED\",\"routeOrder\":1}]")})
            rejectChanged(KAFKA, changed);
    }

    private static void rejectChanged(String baseline, String changed) {
        assertNotEquals(baseline, changed);
        assertThrows(IllegalArgumentException.class, () -> verify(changed));
    }

    private static void verify(String evidence) {
        LifecycleEvidence.verify(JSON.readTree(evidence));
    }
}

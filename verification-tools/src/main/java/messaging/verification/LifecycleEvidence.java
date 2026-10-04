package messaging.verification;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Validate the durable lifecycle outbox and its physical Kafka publication. */
final class LifecycleEvidence {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private LifecycleEvidence() {}

    static void run() throws Exception {
        verify(JSON.readTree(System.in));
        System.out.println("{\"consistent\":true}");
    }

    static void verify(JsonNode evidence) {
        switch (evidence.path("kind").asText()) {
            case "final" -> verifyFinal(evidence);
            case "kafka" -> verifyKafka(evidence);
            default -> throw new IllegalArgumentException("Lifecycle evidence mismatch: unknown_kind");
        }
    }

    private static void verifyFinal(JsonNode evidence) {
        JsonNode item = evidence.path("outbox"), result = evidence.path("result");
        require(item.isObject() && result.isObject(), "outbox_missing");
        require(!item.has("lifecycle_bucket"), "lifecycle_index_retained");
        require("PUBLISHED".equals(item.path("publish_state").path("S").asText()), "outbox_not_published");
        try {
            require(result.equals(JSON.readTree(item.path("result_event").path("S").asText())), "outbox_result_mismatch");
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException("Lifecycle evidence mismatch: invalid_result_event", exception);
        }
        require(result.path("outcome").asText().equals(evidence.path("outcome").asText())
                && number(result, "routeOrder") == number(evidence, "route"), "final_result");
        require(!result.path("deliveryId").asText().isBlank(), "delivery_id");
        checkProvider(evidence.path("primaryProvider"), number(evidence, "primaryCalls"));
        checkProvider(evidence.path("secondaryProvider"), number(evidence, "secondaryCalls"));
    }

    private static void checkProvider(JsonNode provider, long expected) {
        if (expected == 0 && provider.isNull()) return;
        require(provider.isObject() && number(provider, "calls") == expected, "provider_calls");
    }

    private static void verifyKafka(JsonNode evidence) {
        JsonNode expected = evidence.path("expected"), records = evidence.path("records");
        require(expected.isObject() && records.isArray(), "kafka_evidence_missing");
        require(records.size() == number(evidence, "endOffset"), "physical_record_count");
        require(records.size() == expected.size(), "duplicate_or_missing_final_record");
        for (JsonNode record : records) {
            String delivery = record.path("deliveryId").asText();
            require(!delivery.isBlank() && expected.has(delivery), "unexpected_delivery");
            require(record.equals(expected.get(delivery)), "kafka_result_mismatch");
        }
    }

    private static long number(JsonNode row, String field) {
        require(row.path(field).isNumber(), "missing_" + field);
        return row.path(field).asLong();
    }

    private static void require(boolean condition, String reason) {
        if (!condition) throw new IllegalArgumentException("Lifecycle evidence mismatch: " + reason);
    }
}

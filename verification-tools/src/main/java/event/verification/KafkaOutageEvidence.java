package event.verification;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.OffsetDateTime;

/** Reconcile the retained expiry while an owned Kafka broker is stopped. */
final class KafkaOutageEvidence {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private KafkaOutageEvidence() {}

    static void run() throws Exception {
        verify(JSON.readTree(System.in));
        System.out.println("{\"consistent\":true}");
    }

    static void verify(JsonNode evidence) {
        require("exited".equals(evidence.path("kafka").path("status").asText()), "kafka_status");
        require(evidence.path("kafka").path("running").isBoolean()
                && !evidence.path("kafka").path("running").asBoolean(), "kafka_running");
        for (String field : new String[]{"publishFailuresBefore", "publishFailuresAfter", "historyRowsBefore", "historyRows",
                "customerRequestsBefore", "customerRequests", "completedRedisTtl"})
            require(evidence.path(field).isNumber(), "missing_" + field);
        require(evidence.path("publishFailuresAfter").asLong() > evidence.path("publishFailuresBefore").asLong(), "publish_failure");
        require(evidence.path("historyRows").asLong() == evidence.path("historyRowsBefore").asLong(), "early_history");
        require(evidence.path("customerRequests").asLong() == evidence.path("customerRequestsBefore").asLong(), "early_notification");
        require(evidence.path("completedRedisTtl").asLong() == -2, "early_completion");

        JsonNode result = JSON.readTree(evidence.path("step").path("result_event").path("S").asText());
        require(evidence.path("origin").path("delivery_id").path("S").asText().equals(result.path("deliveryId").asText()), "delivery_id");
        require(evidence.path("requestKey").asText().equals(result.path("requestKey").asText()), "request_key");
        require("EXPIRED".equals(result.path("outcome").asText()) && result.path("routeOrder").asInt() == 1, "expiry_outcome");
        String deadline = result.path("deadline").asText();
        require(deadline.equals(result.path("resultAt").asText()), "result_deadline");
        require(OffsetDateTime.parse(deadline).toInstant().toEpochMilli()
                == Long.parseLong(evidence.path("step").path("deadline_at").path("N").asText()), "stored_deadline");

        JsonNode provider = evidence.path("provider");
        require(provider.isArray() && provider.size() == 2
                && provider.get(0).path("calls").isNumber() && provider.get(0).path("effects").isNumber()
                && provider.get(1).path("calls").isNumber() && provider.get(1).path("effects").isNumber()
                && provider.get(0).path("calls").asInt() == 1 && provider.get(0).path("effects").asInt() == 1
                && provider.get(1).path("calls").asInt() == 0 && provider.get(1).path("effects").asInt() == 0, "provider_effects");
    }

    private static void require(boolean condition, String reason) {
        if (!condition) throw new IllegalArgumentException("Kafka outage evidence mismatch: " + reason);
    }
}

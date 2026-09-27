package event.verification;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Reconcile durable state across real JVM termination and restart scenarios. */
final class ProcessRecoveryEvidence {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private ProcessRecoveryEvidence() {}

    static void run() throws Exception {
        JsonNode evidence = JSON.readTree(System.in);
        switch (evidence.path("kind").asText()) {
            case "ack-loss" -> verifyAckLoss(evidence);
            case "uncertain-step" -> verifyUncertainStep(evidence);
            default -> throw new IllegalArgumentException("Unknown process evidence kind: " + evidence.path("kind").asText());
        }
        System.out.println("{\"consistent\":true}");
    }

    static void verifyAckLoss(JsonNode evidence) {
        JsonNode before = evidence.path("before"), after = evidence.path("after"), callbacks = evidence.path("callbacks");
        require("IN_FLIGHT".equals(before.path("status").asText()) && number(before, "attempt_count") == 1, "initial_batch");
        require("DELIVERED".equals(after.path("status").asText()) && number(after, "attempt_count") == 2, "confirmed_retry");
        String batch = before.path("batch_id").asText();
        String body = before.path("request_body").asText();
        require(!batch.isBlank() && !body.isBlank() && batch.equals(after.path("batch_id").asText())
                && body.equals(after.path("request_body").asText()), "same_durable_batch");
        require(callbacks.isArray() && callbacks.size() == 2
                && number(callbacks.get(0), "status") == 0 && number(callbacks.get(1), "status") == 204, "customer_ack_sequence");
        JsonNode stored = JSON.readTree(body);
        require(stored.equals(callbacks.get(0).path("body")) && stored.equals(callbacks.get(1).path("body")), "same_customer_body");
    }

    static void verifyUncertainStep(JsonNode evidence) {
        JsonNode before = evidence.path("before"), after = evidence.path("after"), provider = evidence.path("provider");
        for (String field : new String[]{"status", "version", "retry_count", "deadline_at", "lease_until"}) {
            require(!before.path(field).isMissingNode() && before.path(field).equals(after.path(field)), "changed_" + field);
        }
        require("PROCESSING".equals(after.path("status").path("S").asText())
                && "0".equals(after.path("retry_count").path("N").asText()), "uncertain_state");
        require(number(provider, "calls") == 1 && number(provider, "effects") == 1, "provider_effects");
    }

    private static long number(JsonNode row, String field) {
        require(row.path(field).isNumber(), "missing_" + field);
        return row.path(field).asLong();
    }

    private static void require(boolean condition, String reason) {
        if (!condition) throw new IllegalArgumentException("Process recovery evidence mismatch: " + reason);
    }
}

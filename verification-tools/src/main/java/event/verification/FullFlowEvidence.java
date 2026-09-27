package event.verification;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Reconcile the durable customer result with provider, cleanup, and completion evidence. */
final class FullFlowEvidence {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private FullFlowEvidence() {}

    static void run() throws Exception {
        verify(JSON.readTree(System.in));
        System.out.println("{\"consistent\":true}");
    }

    static void verify(JsonNode evidence) {
        JsonNode row = evidence.path("row"), result = row.path("result_json");
        require("DELIVERED".equals(row.path("notification_status").asText())
                && "DONE".equals(row.path("cleanup_status").asText()), "durable_completion");
        require(result.path("outcome").asText().equals(evidence.path("outcome").asText())
                && number(result, "routeOrder") == number(evidence, "route"), "result_outcome");
        String delivery = result.path("deliveryId").asText();
        require(!delivery.isBlank(), "delivery_id");
        JsonNode expected = evidence.path("expectedCalls"), provider = evidence.path("provider");
        require(expected.isArray() && provider.isArray() && expected.size() == provider.size(), "provider_routes");
        for (int index = 0; index < expected.size(); index++)
            require(expected.get(index).isNumber() && number(provider.get(index), "calls") == expected.get(index).asLong(), "provider_calls");
        require(empty(evidence.path("origin")) && empty(evidence.path("steps")), "uncompacted_items");
        require(number(evidence, "completedTtl") > 0, "completed_marker");

        JsonNode callbacks = evidence.path("callbacks");
        require(callbacks.isArray(), "callbacks_missing");
        boolean received = false;
        for (JsonNode callback : callbacks) {
            if (number(callback, "status") != 204) continue;
            JsonNode results = callback.path("body").path("results");
            require(results.isArray(), "callback_results_missing");
            for (JsonNode item : results) {
                if (delivery.equals(item.path("deliveryId").asText())) {
                    require(result.equals(item), "customer_result_mismatch");
                    received = true;
                }
            }
        }
        require(received, "customer_ack_missing");
    }

    private static long number(JsonNode row, String field) {
        require(row.path(field).isNumber(), "missing_" + field);
        return row.path(field).asLong();
    }

    private static boolean empty(JsonNode value) {
        return value.isNull() || (value.isObject() || value.isArray()) && value.size() == 0;
    }

    private static void require(boolean condition, String reason) {
        if (!condition) throw new IllegalArgumentException("Full-flow evidence mismatch: " + reason);
    }
}

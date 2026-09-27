package event.verification;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Confirm durable delivery and cleanup while the Redis completion marker is unavailable. */
final class RedisOutageEvidence {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private RedisOutageEvidence() {}

    static void run() throws Exception {
        verify(JSON.readTree(System.in));
        System.out.println("{\"consistent\":true}");
    }

    static void verify(JsonNode evidence) {
        JsonNode row = evidence.path("row");
        JsonNode result = row.path("result_json");
        require(result.path("outcome").asText().equals(evidence.path("expected").asText())
                && result.path("routeOrder").asInt() == 1, "result");
        require("DELIVERED".equals(row.path("notification_status").asText())
                && "DONE".equals(row.path("cleanup_status").asText()), "durable_completion");
        JsonNode provider = evidence.path("provider");
        require(provider.isArray() && provider.size() == 2
                && count(provider.get(0), "calls") == 1 && count(provider.get(0), "effects") == 1
                && count(provider.get(1), "calls") == 0 && count(provider.get(1), "effects") == 0, "provider_effects");
        require(empty(evidence.path("origin")) && empty(evidence.path("steps")), "uncompacted_items");

        boolean received = false;
        JsonNode callbacks = evidence.path("callbacks");
        require(callbacks.isArray(), "callbacks_missing");
        for (JsonNode callback : callbacks) {
            if (callback.path("status").asInt() != 204) continue;
            for (JsonNode item : callback.path("body").path("results")) {
                if (item.path("deliveryId").asText().equals(result.path("deliveryId").asText())) {
                    require(item.equals(result), "customer_result_mismatch");
                    received = true;
                }
            }
        }
        require(received, "customer_ack_missing");
    }

    private static int count(JsonNode row, String field) {
        require(row.path(field).isNumber(), "provider_" + field + "_missing");
        return row.path(field).asInt();
    }

    private static boolean empty(JsonNode value) {
        return value.isNull() || (value.isObject() || value.isArray()) && value.size() == 0;
    }

    private static void require(boolean condition, String reason) {
        if (!condition) throw new IllegalArgumentException("Redis outage evidence mismatch: " + reason);
    }
}

package messaging.verification;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Reconcile durable results and Kafka offset retention during a local PostgreSQL outage. */
final class PostgresOutageEvidence {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private PostgresOutageEvidence() {}

    static void run() throws Exception {
        verify(JSON.readTree(System.in));
        System.out.println("{\"consistent\":true}");
    }

    static void verify(JsonNode evidence) {
        JsonNode before = evidence.path("before"), during = evidence.path("during");
        require("exited".equals(during.path("postgres").path("status").asText())
                && during.path("postgres").path("running").isBoolean()
                && !during.path("postgres").path("running").asBoolean(), "postgres_stopped");
        require(during.path("sqlProbeRejected").isBoolean() && during.path("sqlProbeRejected").asBoolean(), "sql_probe");
        require(before.path("applicationPids").isObject()
                && before.path("applicationPids").equals(during.path("applicationPids")), "application_pids");
        require(number(during, "storeFailures") >= number(before, "storeFailures") + 2, "sql_store_failures");
        require(number(during, "customerRequests") == 0, "early_notification");
        JsonNode requests = during.path("requests");
        require(requests.isArray() && requests.size() == 2, "request_count");
        require(offsets(before.path("offsets")).equals(offsets(during.path("offsets"))), "committed_offsets");
        long lag = 0;
        for (JsonNode offset : during.path("offsets")) lag += number(offset, "lag");
        require(lag >= 2, "retained_lag");

        Set<String> outcomes = new HashSet<>();
        for (JsonNode row : requests) {
            outcomes.add(row.path("expectedOutcome").asText());
            JsonNode result = JSON.readTree(row.path("step").path("result_event").path("S").asText());
            require(row.path("origin").path("delivery_id").path("S").asText().equals(result.path("deliveryId").asText()), "delivery_id");
            require(row.path("requestKey").asText().equals(result.path("requestKey").asText()), "request_key");
            require(row.path("expectedOutcome").asText().equals(result.path("outcome").asText())
                    && number(result, "routeOrder") == 1, "outcome");
            if ("EXPIRED".equals(result.path("outcome").asText())) {
                String deadline = result.path("deadline").asText();
                require(deadline.equals(result.path("resultAt").asText()), "expiry_time");
                require(OffsetDateTime.parse(deadline).toInstant().toEpochMilli()
                        == Long.parseLong(row.path("step").path("deadline_at").path("N").asText()), "stored_deadline");
            }
            JsonNode provider = row.path("provider");
            require(provider.isArray() && provider.size() == 2
                    && number(provider.get(0), "calls") == 1 && number(provider.get(0), "effects") == 1
                    && number(provider.get(1), "calls") == 0 && number(provider.get(1), "effects") == 0, "provider_effects");
            require(number(row, "completedRedisTtl") == -2, "early_completion");
        }
        require(outcomes.equals(Set.of("DELIVERED", "EXPIRED")), "outcome_coverage");
    }

    private static Map<Integer, Long> offsets(JsonNode values) {
        require(values.isArray(), "offsets_missing");
        Map<Integer, Long> offsets = new HashMap<>();
        for (JsonNode row : values) offsets.put((int) number(row, "partition"), Math.max(0, number(row, "committed")));
        return offsets;
    }

    private static long number(JsonNode row, String field) {
        require(row.path(field).isNumber(), "missing_" + field);
        return row.path(field).asLong();
    }

    private static void require(boolean condition, String reason) {
        if (!condition) throw new IllegalArgumentException("PostgreSQL outage evidence mismatch: " + reason);
    }
}

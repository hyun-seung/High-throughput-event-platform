package messaging.verification;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;

/** Ensure DynamoDB loss blocks claims or retains an accepted result without early completion. */
final class DynamoOutageEvidence {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private DynamoOutageEvidence() {}

    static void run() throws Exception {
        verify(JSON.readTree(System.in));
        System.out.println("{\"consistent\":true}");
    }

    static void verify(JsonNode evidence) {
        JsonNode before = evidence.path("before"), during = evidence.path("during");
        long calls = number(evidence, "calls");
        String failure = evidence.path("failure").asText();
        require((calls == 0 && "claimFailed".equals(failure))
                || (calls == 1 && "lifecycleFailed".equals(failure)), "case");
        require("exited".equals(during.path("dynamo").path("status").asText())
                && during.path("dynamo").path("running").isBoolean()
                && !during.path("dynamo").path("running").asBoolean(), "dynamo_stopped");
        require(before.path("applicationPids").isObject()
                && before.path("applicationPids").equals(during.path("applicationPids")), "application_pids");
        require(number(during.path("metrics"), failure) > number(before.path("metrics"), failure), "failure_metric");
        JsonNode provider = during.path("provider");
        require(provider.isArray() && provider.size() == 2
                && number(provider.get(0), "calls") == calls && number(provider.get(0), "effects") == calls
                && number(provider.get(1), "calls") == 0 && number(provider.get(1), "effects") == 0, "provider_effects");
        require(number(during, "historyRows") == 0 && number(during, "customerResults") == 0, "early_result");
        require(number(during, "completedRedisTtl") == -2, "early_completion");
        if (calls == 0) {
            require(offsets(before.path("dispatchOffsets")).equals(offsets(during.path("dispatchOffsets"))), "committed_offsets");
            long lag = 0;
            for (JsonNode partition : during.path("dispatchOffsets")) lag += number(partition, "lag");
            require(lag > 0, "retained_dispatch_lag");
        }
    }

    private static Map<Long, Long> offsets(JsonNode rows) {
        require(rows.isArray(), "offsets_missing");
        Map<Long, Long> offsets = new HashMap<>();
        for (JsonNode row : rows) offsets.put(number(row, "partition"), Math.max(0, number(row, "committed")));
        return offsets;
    }

    private static long number(JsonNode row, String field) {
        require(row.path(field).isNumber(), "missing_" + field);
        return row.path(field).asLong();
    }

    private static void require(boolean condition, String reason) {
        if (!condition) throw new IllegalArgumentException("DynamoDB outage evidence mismatch: " + reason);
    }
}

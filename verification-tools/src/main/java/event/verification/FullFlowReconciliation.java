package event.verification;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Reconciles one load phase using the client, SQL and customer receipt evidence. */
final class FullFlowReconciliation {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final List<String> STAGES = List.of("providerResult", "finalized", "sqlStored", "customerReceived", "cleanup");

    private FullFlowReconciliation() {}

    static void run() throws Exception {
        JsonNode input = JSON.readTree(System.in);
        System.out.println(JSON.writeValueAsString(reconcile(input)));
    }

    static Map<String, Object> reconcile(JsonNode input) {
        JsonNode starts = input.get("starts");
        JsonNode responses = input.get("responses");
        JsonNode history = input.get("history");
        JsonNode callbacks = input.get("callbacks");
        Set<String> expected = new HashSet<>();
        Map<String, Double> startsByRequest = new HashMap<>();
        for (var response : responses.properties()) {
            JsonNode value = response.getValue();
            String request = value.path("deliveryId").asText("");
            if (!starts.has(response.getKey()) || value.path("status").asInt() != 202 || request.isEmpty())
                throw new IllegalArgumentException("Every unique input needs a confirmed API requestKey");
            expected.add(request);
            startsByRequest.put(request, starts.path(response.getKey()).path("started").asDouble() / 1000);
        }
        if (expected.size() != starts.size())
            throw new IllegalArgumentException("Every unique input needs a confirmed API requestKey");

        List<JsonNode> rows = new ArrayList<>();
        Set<String> observed = new HashSet<>();
        for (JsonNode row : history) {
            String request = row.path("result_json").path("requestKey").asText();
            if (expected.contains(request)) {
                rows.add(row);
                observed.add(request);
            }
        }
        if (rows.size() != expected.size() || !observed.equals(expected))
            throw new IllegalArgumentException("Input/history set mismatch or duplicate execution");

        Map<String, JsonNode> received = new HashMap<>();
        Map<String, Double> receivedAt = new HashMap<>();
        for (JsonNode batch : callbacks) {
            if (batch.path("status").asInt() != 204)
                throw new IllegalArgumentException("Unexpected customer response in normal load test");
            for (JsonNode result : batch.path("body").path("results")) {
                if (!expected.contains(result.path("requestKey").asText())) continue;
                String execution = result.path("deliveryId").asText();
                if (received.containsKey(execution) && !received.get(execution).equals(result))
                    throw new IllegalArgumentException("Conflicting customer result");
                received.putIfAbsent(execution, result);
                receivedAt.putIfAbsent(execution, epoch(batch.path("receivedAt").asText()));
            }
        }

        Map<String, List<Double>> latencies = new LinkedHashMap<>();
        STAGES.forEach(stage -> latencies.put(stage, new ArrayList<>()));
        for (JsonNode row : rows) {
            JsonNode result = row.path("result_json");
            String execution = result.path("deliveryId").asText();
            if (!"DELIVERED".equals(result.path("outcome").asText()) || result.path("routeOrder").asInt() != 1)
                throw new IllegalArgumentException("Unexpected delivery result");
            if (!"DELIVERED".equals(row.path("notification_status").asText())
                    || !"DONE".equals(row.path("cleanup_status").asText()))
                throw new IllegalArgumentException("Incomplete SQL workflow");
            if (!result.equals(received.get(execution)))
                throw new IllegalArgumentException("Missing/conflicting customer receipt");
            double start = startsByRequest.get(result.path("requestKey").asText());
            add(latencies, "providerResult", start, epoch(result.path("resultAt").asText()));
            add(latencies, "finalized", start, epoch(result.path("finalizedAt").asText()));
            add(latencies, "sqlStored", start, epoch(row.path("stored_at").asText()));
            add(latencies, "customerReceived", start, receivedAt.get(execution));
            add(latencies, "cleanup", start, epoch(row.path("cleanup_completed_at").asText()));
        }
        Map<String, Object> stats = new LinkedHashMap<>();
        for (var entry : latencies.entrySet()) {
            List<Double> values = entry.getValue();
            values.sort(Double::compare);
            stats.put(entry.getKey(), Map.of("p50", percentile(values, .5), "p95", percentile(values, .95),
                    "p99", percentile(values, .99), "max", percentile(values, 1)));
        }
        return Map.of("rows", rows, "stats", stats);
    }

    private static void add(Map<String, List<Double>> latencies, String stage, double start, double end) {
        if (end < start) throw new IllegalArgumentException("Negative wall-clock latency");
        latencies.get(stage).add((end - start) * 1000);
    }

    private static double epoch(String iso) {
        var instant = OffsetDateTime.parse(iso).toInstant();
        return instant.getEpochSecond() + instant.getNano() / 1_000_000_000d;
    }

    private static double percentile(List<Double> values, double fraction) {
        double index = (values.size() - 1) * fraction;
        int low = (int) index;
        int high = (int) Math.ceil(index);
        return values.get(low) + (values.get(high) - values.get(low)) * (index - low);
    }
}

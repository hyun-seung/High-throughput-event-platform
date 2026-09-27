package event.verification;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

/** Request-level evidence reconciliation for the isolated API-to-provider PoC. */
final class PocReconciliation {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String REQUESTED = "delivery.requested.v1";
    private static final String DISPATCH = "delivery.dispatch-requested.v1";
    private static final String DLT = "delivery.requested.dlt.v1";
    private static final List<String> TOPICS = List.of(REQUESTED, DISPATCH, DLT);

    private PocReconciliation() {}

    static void run() throws Exception {
        System.out.println(JSON.writeValueAsString(reconcile(JSON.readTree(System.in))));
    }

    static Map<String, Object> reconcile(JsonNode input) {
        JsonNode starts = input.path("starts");
        JsonNode results = input.path("results");
        Map<String, List<JsonNode>> byKey = new LinkedHashMap<>();
        for (var start : starts.properties()) {
            String key = start.getValue().path("key").asText();
            JsonNode response = results.path(start.getKey());
            byKey.computeIfAbsent(key, ignored -> new ArrayList<>()).add(response.isMissingNode() ? JSON.readTree("{\"status\":0,\"deliveryId\":null}") : response);
        }

        Map<String, Map<String, Integer>> kafka = new LinkedHashMap<>();
        TOPICS.forEach(topic -> kafka.put(topic, new HashMap<>()));
        Map<String, Set<String>> dispatchExecutions = new HashMap<>();
        for (JsonNode record : input.path("records")) {
            if (record.path("deliveryId").isNull() || record.path("deliveryId").isMissingNode())
                throw new IllegalArgumentException("Unparseable Kafka record cannot be silently omitted");
            String topic = record.path("topic").asText();
            Map<String, Integer> counts = kafka.get(topic);
            if (counts == null) throw new IllegalArgumentException("Unexpected Kafka topic: " + topic);
            String request = record.path("requestKey").asText("");
            if (request.isEmpty()) request = record.path("deliveryId").asText();
            counts.merge(request, 1, Integer::sum);
            if (DISPATCH.equals(topic)) dispatchExecutions.computeIfAbsent(request, ignored -> new HashSet<>())
                    .add(record.path("deliveryId").asText());
        }

        Map<String, JsonNode> items = new HashMap<>();
        for (JsonNode entry : input.path("items"))
            items.put(itemKey(entry.path("pk").asText(), entry.path("sk").asText()), entry.path("item"));
        JsonNode provider = input.path("provider");
        List<Map<String, Object>> rows = new ArrayList<>();
        List<Double> latencies = new ArrayList<>();
        for (var entry : byKey.entrySet()) {
            String key = entry.getKey();
            String delivery = id("delivery:" + input.path("tenant").asText() + ":" + key);
            JsonNode meta = items.get(itemKey("DELIVERY#" + delivery, "META"));
            String executionId = meta == null ? delivery : meta.path("delivery_id").path("S").asText(delivery);
            String attempt = id("attempt:" + executionId + ":mock-provider:1:1");
            JsonNode execution = items.get(itemKey("DELIVERY#" + executionId, "ATTEMPT#" + attempt));
            String state = execution == null ? null : execution.path("status").path("S").asText(null);
            if (state == null) state = meta != null ? "META_ONLY" : count(kafka, DLT, delivery) > 0 ? "DLT"
                    : count(kafka, REQUESTED, delivery) > 0 ? "KAFKA_ONLY" : "UNEXPLAINED";
            List<String> problems = new ArrayList<>();
            List<Integer> statuses = new ArrayList<>();
            for (JsonNode response : entry.getValue()) {
                int status = response.path("status").asInt();
                statuses.add(status);
                if (status != 202 && !problems.contains("http_unconfirmed_or_rejected"))
                    problems.add("http_unconfirmed_or_rejected");
            }
            for (JsonNode response : entry.getValue()) {
                if (response.path("status").asInt() == 202 && !delivery.equals(response.path("deliveryId").asText())
                        && !problems.contains("response_id_mismatch")) problems.add("response_id_mismatch");
            }
            if (!"ACCEPTED".equals(state) || meta == null) problems.add("not_persisted_accepted");
            if (count(kafka, REQUESTED, delivery) == 0 || count(kafka, DISPATCH, delivery) == 0)
                problems.add("missing_kafka_evidence");
            if (count(kafka, DLT, delivery) > 0) problems.add("dlt_present");
            if (dispatchExecutions.getOrDefault(delivery, Set.of()).size() > 1)
                problems.add("multiple_execution_generations");
            JsonNode countsNode = provider.path(attempt);
            Object counts = countsNode.isMissingNode() ? Map.of("calls", 0, "effects", 0) : countsNode;
            if (countsNode.isMissingNode() || countsNode.size() != 2
                    || countsNode.path("calls").asInt() != 1 || countsNode.path("effects").asInt() != 1)
                problems.add("provider_call_or_effect_mismatch");
            Double latency = null;
            if (meta != null && execution != null && "ACCEPTED".equals(state)) {
                var begin = OffsetDateTime.parse(meta.path("occurred_at").path("S").asText()).toInstant();
                var end = OffsetDateTime.parse(execution.path("updated_at").path("S").asText()).toInstant();
                latency = Duration.between(begin, end).toNanos() / 1_000_000d;
                if (latency < 0) problems.add("invalid_wall_clock");
                else latencies.add(latency);
            }
            Map<String, Object> topicCounts = new LinkedHashMap<>();
            for (String topic : TOPICS) topicCounts.put(topic, count(kafka, topic, delivery));
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("key", key);
            row.put("deliveryId", delivery);
            row.put("executionId", executionId);
            row.put("attemptId", attempt);
            row.put("state", state);
            row.put("httpStatuses", statuses);
            row.put("provider", counts);
            row.put("kafka", topicCounts);
            row.put("persistedTimestampLatencyMs", latency);
            row.put("problems", problems);
            rows.add(row);
        }

        Set<String> unexpected = new TreeSet<>();
        kafka.values().forEach(topic -> unexpected.addAll(topic.keySet()));
        rows.forEach(row -> unexpected.remove(row.get("deliveryId")));
        Map<String, Integer> statuses = new TreeMap<>();
        for (var result : results.properties()) statuses.merge(result.getValue().path("status").asText(), 1, Integer::sum);
        Map<String, Integer> states = new TreeMap<>();
        rows.forEach(row -> states.merge((String) row.get("state"), 1, Integer::sum));
        int problemRequests = (int) rows.stream().filter(row -> !((List<?>) row.get("problems")).isEmpty()).count();
        latencies.sort(Double::compare);
        Map<String, Object> latencyStats = new LinkedHashMap<>();
        latencyStats.put("p50", percentile(latencies, .50));
        latencyStats.put("p95", percentile(latencies, .95));
        latencyStats.put("p99", percentile(latencies, .99));
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("uniqueRequests", rows.size());
        summary.put("submitted", starts.size());
        summary.put("responses", results.size());
        summary.put("httpStatuses", statuses);
        summary.put("unanswered", starts.size() - results.size());
        summary.put("states", states);
        summary.put("problemRequests", problemRequests);
        summary.put("unexpectedKafkaIds", new ArrayList<>(unexpected));
        summary.put("persistedTimestampLatencyMs", latencyStats);
        summary.put("consistent", problemRequests == 0 && unexpected.isEmpty() && results.size() == starts.size());
        return Map.of("rows", rows, "summary", summary);
    }

    private static String itemKey(String pk, String sk) { return pk + "\u0000" + sk; }

    private static int count(Map<String, Map<String, Integer>> kafka, String topic, String request) {
        return kafka.get(topic).getOrDefault(request, 0);
    }

    private static String id(String text) {
        return UUID.nameUUIDFromBytes(text.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private static Double percentile(List<Double> values, double fraction) {
        if (values.isEmpty()) return null;
        double index = (values.size() - 1) * fraction;
        int low = (int) index;
        return values.get(low) + (values.get(Math.min(low + 1, values.size() - 1)) - values.get(low)) * (index - low);
    }
}

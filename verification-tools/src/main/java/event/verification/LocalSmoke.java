package event.verification;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/** Verify local API, idempotency, persisted dispatch state, and optionally metrics. */
final class LocalSmoke {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Pattern LABEL = Pattern.compile("([a-zA-Z_][a-zA-Z_0-9]*)=\"([^\"\\\\]*)\"");

    private LocalSmoke() {}

    record Config(String api, String dynamo, Map<String, String> management) {
        static Config environment() {
            var ports = new LinkedHashMap<String, String>();
            ports.put("api", System.getenv().getOrDefault("DELIVERY_API_METRICS_PORT", "19080"));
            ports.put("ingress", System.getenv().getOrDefault("INGRESS_METRICS_PORT", "19081"));
            ports.put("dispatch", System.getenv().getOrDefault("DISPATCH_METRICS_PORT", "19082"));
            ports.put("simulator", System.getenv().getOrDefault("SIMULATOR_METRICS_PORT", "19090"));
            var urls = new LinkedHashMap<String, String>();
            ports.forEach((name, port) -> urls.put(name, "http://127.0.0.1:" + port));
            return new Config("http://localhost:" + System.getenv().getOrDefault("DELIVERY_API_PORT", "8080"),
                    "http://localhost:" + System.getenv().getOrDefault("DYNAMODB_HOST_PORT", "8000"), urls);
        }
    }

    interface StateReader {
        JsonNode query(String deliveryId, String table) throws Exception;
    }

    static void run(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1 && !args[0].equals("--metrics")))
            throw new IllegalArgumentException("Usage: local-smoke [--metrics]");
        var proxy = new ProxySelector() {
            @Override public List<Proxy> select(URI uri) { return List.of(Proxy.NO_PROXY); }
            @Override public void connectFailed(URI uri, java.net.SocketAddress address, IOException error) {}
        };
        var config = Config.environment();
        try (var client = HttpClient.newBuilder().proxy(proxy).connectTimeout(Duration.ofSeconds(5)).build()) {
            for (String line : check(config, args.length == 1, client, (id, table) -> queryState(config.dynamo(), id, table)))
                System.out.println(line);
        }
    }

    static List<String> check(Config config, boolean metrics, HttpClient client, StateReader state) throws Exception {
        var messages = new ArrayList<String>();
        String token = post(client, config.api() + "/api/v1/auth/token",
                Map.of("username", "local-user", "password", "local-password"), 200, null, null)
                .get("accessToken").asText();
        messages.add("PASS: local-user authentication");
        Map<String, String> before = null;
        if (metrics) {
            var unauthorized = get(client, config.management().get("api") + "/actuator/prometheus", null);
            if (unauthorized.statusCode() != 401)
                throw new IllegalStateException("API management endpoint unexpectedly accessible without authentication");
            if (JSON.readTree(checked(get(client, config.management().get("simulator") + "/actuator/simulator", null), 200))
                    .get("deduplicate").asBoolean())
                throw new IllegalStateException("Metrics smoke requires SIMULATOR_DEDUPLICATE=false");
            before = snapshot(config, token, client);
        }
        String key = "local-smoke-" + UUID.randomUUID().toString().replace("-", "");
        var payload = Map.of("deliveryType", "EMAIL", "payload", Map.of("message", "local smoke verification"));
        String url = config.api() + "/api/v1/deliveries";
        String first = post(client, url, payload, 202, token, key).get("deliveryId").asText();
        String second = post(client, url, payload, 202, token, key).get("deliveryId").asText();
        if (!first.equals(second)) throw new IllegalStateException("Repeated idempotency key returned a different deliveryId");
        messages.add("PASS: two HTTP 202 responses with deliveryId=" + first);
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            JsonNode meta = null;
            for (JsonNode item : state.query(first, "ORIGIN"))
                if (item.path("sk").path("S").asText().equals("META")) meta = item;
            String execution = meta == null ? first : meta.path("delivery_id").path("S").asText(first);
            JsonNode attempt = null;
            int attempts = 0;
            for (JsonNode item : state.query(execution, "STEP")) {
                if (item.path("sk").path("S").asText().startsWith("ATTEMPT#")) {
                    attempt = item;
                    attempts++;
                }
            }
            if (attempts > 1) throw new IllegalStateException("More than one dispatch attempt exists");
            if (meta != null && attempt != null && attempt.path("status").path("S").asText().equals("ACCEPTED")) {
                if (!attempt.has("provider_processed_at"))
                    throw new IllegalStateException("Accepted attempt has no provider timestamp");
                messages.add("PASS: DynamoDB META and one ACCEPTED attempt with provider timestamp");
                if (metrics) {
                    verifyMetrics(config, token, before, attempt, client);
                    messages.add("PASS: stage/HTTP/DB metrics, latency histograms, duplicate skip and one provider effect without deduplication");
                }
                return messages;
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException("Timed out waiting for META and ACCEPTED attempt; inspect worker logs");
    }

    private static JsonNode queryState(String dynamo, String delivery, String table) throws Exception {
        var query = Map.of("TableName", table, "ConsistentRead", true,
                "KeyConditionExpression", "pk = :pk",
                "ExpressionAttributeValues", Map.of(":pk", Map.of("S", "DELIVERY#" + delivery)));
        var command = List.of("curl", "--noproxy", "*", "--max-time", "5", "--silent", "--show-error",
                "--fail-with-body", "--aws-sigv4", "aws:amz:ap-northeast-2:dynamodb",
                "--user", "local:local", "-H", "Content-Type: application/x-amz-json-1.0",
                "-H", "X-Amz-Target: DynamoDB_20120810.Query", "-d", JSON.writeValueAsString(query), dynamo + "/");
        var process = new ProcessBuilder(command).start();
        String body = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String error = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        if (process.waitFor() != 0) throw new IOException("DynamoDB query failed: " + error);
        return JSON.readTree(body).get("Items");
    }

    private static JsonNode post(HttpClient client, String url, Object payload, int expected,
                                 String token, String key) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(payload), StandardCharsets.UTF_8));
        if (token != null) builder.header("Authorization", "Bearer " + token);
        if (key != null) builder.header("Idempotency-Key", key);
        return JSON.readTree(checked(client.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)), expected))
                .get("data");
    }

    private static HttpResponse<String> get(HttpClient client, String url, String token) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5)).GET();
        if (token != null) builder.header("Authorization", "Bearer " + token);
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private static String checked(HttpResponse<String> response, int expected) {
        if (response.statusCode() != expected)
            throw new IllegalStateException("Expected HTTP " + expected + ", got " + response.statusCode());
        return response.body();
    }

    private static Map<String, String> snapshot(Config config, String token, HttpClient client) throws Exception {
        var result = new LinkedHashMap<String, String>();
        for (var entry : config.management().entrySet()) {
            result.put(entry.getKey(), checked(get(client, entry.getValue() + "/actuator/prometheus",
                    entry.getKey().equals("api") ? token : null), 200));
        }
        return result;
    }

    static double metricValue(String scrape, String name, Map<String, String> labels) {
        double total = 0;
        for (String line : scrape.split("\\R")) {
            if (!line.startsWith(name + "{") && !line.startsWith(name + " ")) continue;
            var found = new LinkedHashMap<String, String>();
            var matcher = LABEL.matcher(line);
            while (matcher.find()) found.put(matcher.group(1), matcher.group(2));
            if (labels.entrySet().stream().allMatch(entry -> entry.getValue().equals(found.get(entry.getKey()))))
                total += Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
        }
        return total;
    }

    private record Requirement(String app, String metric, Map<String, String> labels, double expected) {}

    private static void verifyMetrics(Config config, String token, Map<String, String> before,
                                      JsonNode attempt, HttpClient client) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        Map<String, String> after;
        do {
            after = snapshot(config, token, client);
            double duplicates = metricValue(after.get("dispatch"), "delivery_outcomes_total", Map.of("outcome", "dispatch_duplicate"))
                    - metricValue(before.get("dispatch"), "delivery_outcomes_total", Map.of("outcome", "dispatch_duplicate"));
            if (duplicates >= 1) break;
            Thread.sleep(200);
        } while (System.nanoTime() < deadline);
        if (System.nanoTime() >= deadline) throw new IllegalStateException("Duplicate replay was not observed by Dispatch metrics");
        var required = List.of(
                new Requirement("api", "delivery_stage_duration_seconds_count", Map.of("stage", "api_publish", "result", "success"), 2),
                new Requirement("api", "http_server_requests_seconds_count", Map.of("uri", "/api/v1/deliveries", "status", "202"), 2),
                new Requirement("ingress", "delivery_stage_duration_seconds_count", Map.of("stage", "ingress_store", "result", "success"), 2),
                new Requirement("ingress", "delivery_stage_duration_seconds_count", Map.of("stage", "ingress_publish", "result", "success"), 2),
                new Requirement("dispatch", "delivery_outcomes_total", Map.of("outcome", "dispatch_accepted"), 1),
                new Requirement("dispatch", "delivery_acceptance_latency_seconds_count", Map.of(), 1),
                new Requirement("dispatch", "delivery_dynamodb_duration_seconds_count", Map.of("operation", "update_item", "result", "success"), 2),
                new Requirement("dispatch", "delivery_dynamodb_duration_seconds_count", Map.of("operation", "update_item", "result", "condition_failed"), 1));
        for (var item : required) {
            double delta = metricValue(after.get(item.app()), item.metric(), item.labels())
                    - metricValue(before.get(item.app()), item.metric(), item.labels());
            if (delta < item.expected()) throw new IllegalStateException(item.app() + " " + item.metric() + " " + item.labels()
                    + ": expected delta >= " + item.expected() + ", got " + delta);
        }
        for (String app : List.of("api", "ingress", "dispatch"))
            if (!after.get(app).contains("delivery_stage_duration_seconds_bucket{"))
                throw new IllegalStateException(app + ": latency histogram not exposed");
        var counts = JSON.readTree(checked(get(client, config.management().get("simulator")
                + "/actuator/simulator/" + attempt.get("attempt_id").get("S").asText(), null), 200));
        if (counts.path("calls").asInt() != 1 || counts.path("effects").asInt() != 1)
            throw new IllegalStateException("Expected exactly one provider call and effect without provider deduplication, got " + counts);
    }
}

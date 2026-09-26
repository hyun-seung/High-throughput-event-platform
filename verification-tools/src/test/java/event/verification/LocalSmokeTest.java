package event.verification;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class LocalSmokeTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Test
    void verifiesIdempotencyPersistedAttemptMetricsAndProviderEffect() throws Exception {
        var calls = new AtomicInteger();
        var key = new AtomicReference<String>();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/api/v1/auth/token")) reply(exchange, 200, "{\"data\":{\"accessToken\":\"token\"}}");
            else if (path.equals("/api/v1/deliveries")) {
                String observed = exchange.getRequestHeaders().getFirst("Idempotency-Key");
                if (key.get() == null) key.set(observed);
                assertEquals(key.get(), observed);
                assertEquals("Bearer token", exchange.getRequestHeaders().getFirst("Authorization"));
                calls.incrementAndGet();
                reply(exchange, 202, "{\"data\":{\"deliveryId\":\"delivery-1\"}}");
            } else if (path.equals("/api/actuator/prometheus") && exchange.getRequestHeaders().getFirst("Authorization") == null)
                reply(exchange, 401, "{}");
            else if (path.endsWith("/actuator/prometheus")) reply(exchange, 200, metrics(calls.get() == 2));
            else if (path.equals("/simulator/actuator/simulator")) reply(exchange, 200, "{\"deduplicate\":false}");
            else if (path.equals("/simulator/actuator/simulator/attempt-1")) reply(exchange, 200, "{\"calls\":1,\"effects\":1}");
            else reply(exchange, 404, "{}");
        });
        server.start();
        try (var client = HttpClient.newHttpClient()) {
            String base = "http://127.0.0.1:" + server.getAddress().getPort();
            var management = new LinkedHashMap<String, String>();
            for (String app : List.of("api", "ingress", "dispatch", "simulator")) management.put(app, base + "/" + app);
            var config = new LocalSmoke.Config(base, base, management);
            var messages = LocalSmoke.check(config, true, client, (id, table) -> {
                assertEquals("delivery-1", id);
                return JSON.readTree(table.equals("ORIGIN")
                        ? "[{\"sk\":{\"S\":\"META\"}}]"
                        : "[{\"sk\":{\"S\":\"ATTEMPT#attempt-1\"},\"status\":{\"S\":\"ACCEPTED\"},"
                        + "\"attempt_id\":{\"S\":\"attempt-1\"},\"provider_processed_at\":{\"S\":\"now\"}}]");
            });
            assertEquals(4, messages.size());
            assertEquals(2, calls.get());
            assertTrue(messages.getLast().contains("one provider effect"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void parsesLabelledPrometheusSamples() {
        String scrape = "sample{stage=\"a\",result=\"success\"} 2\n"
                + "sample{stage=\"b\",result=\"success\"} 4\n"
                + "sample{stage=\"a\",result=\"failure\"} 5\n";
        assertEquals(2, LocalSmoke.metricValue(scrape, "sample", Map.of("stage", "a", "result", "success")));
        assertEquals(11, LocalSmoke.metricValue(scrape, "sample", Map.of()));
    }

    private static String metrics(boolean after) {
        int two = after ? 2 : 0;
        int one = after ? 1 : 0;
        return "delivery_outcomes_total{outcome=\"dispatch_duplicate\"} " + one + "\n"
                + "delivery_outcomes_total{outcome=\"dispatch_accepted\"} " + one + "\n"
                + "delivery_stage_duration_seconds_count{stage=\"api_publish\",result=\"success\"} " + two + "\n"
                + "delivery_stage_duration_seconds_count{stage=\"ingress_store\",result=\"success\"} " + two + "\n"
                + "delivery_stage_duration_seconds_count{stage=\"ingress_publish\",result=\"success\"} " + two + "\n"
                + "delivery_stage_duration_seconds_bucket{stage=\"api_publish\",le=\"1\"} " + two + "\n"
                + "http_server_requests_seconds_count{uri=\"/api/v1/deliveries\",status=\"202\"} " + two + "\n"
                + "delivery_acceptance_latency_seconds_count " + one + "\n"
                + "delivery_dynamodb_duration_seconds_count{operation=\"update_item\",result=\"success\"} " + two + "\n"
                + "delivery_dynamodb_duration_seconds_count{operation=\"update_item\",result=\"condition_failed\"} " + one + "\n";
    }

    private static void reply(HttpExchange exchange, int status, String body) throws java.io.IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (var output = exchange.getResponseBody()) { output.write(bytes); }
    }
}

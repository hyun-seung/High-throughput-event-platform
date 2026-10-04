package messaging.verification;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MonitoringCollectorTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    @Test
    void staleOrFailedProbeDoesNotPublishOldLag() {
        var row = Map.<String, Object>of("topic", "delivery.requested.v1", "partition", 0,
                "start", 0L, "end", 12L, "lag", 2L, "oldestUncommittedAgeSeconds", 3d);
        var snapshot = Map.<String, Object>of("partitions", List.of(row));
        for (boolean healthy : List.of(false, true)) {
            String stale = MonitoringCollector.exposition(snapshot, 100, healthy, healthy ? 146 : 101);
            assertTrue(stale.contains("platform_kafka_probe_up 0"));
            assertFalse(stale.contains("platform_kafka_committed_lag{"));
        }
        String fresh = MonitoringCollector.exposition(snapshot, 100, true, 110);
        assertTrue(fresh.contains("platform_kafka_probe_up 1"));
        assertTrue(fresh.contains("group=\"delivery-ingress-worker\"} 2"));
        assertTrue(fresh.contains("platform_kafka_retained_records{topic=\"delivery.requested.v1\",partition=\"0\"} 12"));
    }

    @Test
    void refreshesExpiredApiTokenWithoutAnonymousFallback() throws Exception {
        var authCalls = new AtomicInteger();
        var unavailable = new java.util.concurrent.atomic.AtomicBoolean();
        var headers = new ArrayList<String>();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/auth/token", exchange -> {
            int count = authCalls.incrementAndGet();
            reply(exchange, 200, "{\"data\":{\"accessToken\":\"" + (count == 1 ? "expired" : "fresh") + "\"}}");
        });
        server.createContext("/actuator/prometheus", exchange -> {
            String header = exchange.getRequestHeaders().getFirst("Authorization");
            headers.add(header);
            reply(exchange, unavailable.get() ? 503 : "Bearer fresh".equals(header) ? 200 : 401, "metric 1");
        });
        server.start();
        try (var client = HttpClient.newHttpClient()) {
            String base = "http://127.0.0.1:" + server.getAddress().getPort();
            var state = new MonitoringCollector.State(base, base, client);
            assertEquals("metric 1", new String(state.apiMetrics(), StandardCharsets.UTF_8));
            assertEquals(2, authCalls.get());
            assertEquals(List.of("Bearer expired", "Bearer fresh"), headers);
            assertEquals("metric 1", new String(state.apiMetrics(), StandardCharsets.UTF_8));
            assertEquals(2, authCalls.get());
            unavailable.set(true);
            assertThrows(java.io.IOException.class, state::apiMetrics);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void insideCommandsReadPrivateMetricsAndProviderCounts() throws Exception {
        String delivery = "00000000-0000-0000-0000-000000000001";
        String attempt = UUID.nameUUIDFromBytes(("attempt:" + delivery + ":mock-provider:1:1")
                .getBytes(StandardCharsets.UTF_8)).toString();
        assertEquals("7fd23e54-241d-3bf3-b244-5b4b4fd0d323", attempt);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (path.endsWith("/actuator/prometheus")) {
                if (path.startsWith("/api/"))
                    assertEquals("Bearer private-token", exchange.getRequestHeaders().getFirst("Authorization"));
                reply(exchange, 200, "metric 1");
            } else if (path.equals("/simulator/actuator/simulator/" + attempt)) reply(exchange, 200, "{\"calls\":1,\"effects\":1}");
            else if (path.equals("/simulator/actuator/simulator")) reply(exchange, 200, "{\"deduplicate\":false}");
            else reply(exchange, 404, "{}");
        });
        server.start();
        try (var client = HttpClient.newHttpClient()) {
            String base = "http://127.0.0.1:" + server.getAddress().getPort();
            var urls = new java.util.LinkedHashMap<String, String>();
            for (String app : List.of("api", "ingress", "dispatch", "simulator")) urls.put(app, base + "/" + app);
            @SuppressWarnings("unchecked") var metrics = (Map<String, String>) MonitoringCollector.inside(
                    JSON.readTree("{\"mode\":\"metrics\",\"token\":\"private-token\"}"), client, urls, base + "/simulator");
            assertEquals(4, metrics.size());
            assertEquals("metric 1", metrics.get("api"));
            @SuppressWarnings("unchecked") var counts = (Map<String, Object>) MonitoringCollector.inside(
                    JSON.readTree("{\"mode\":\"counts\",\"deliveries\":[\"" + delivery + "\"]}"),
                    client, urls, base + "/simulator");
            assertEquals(1, ((tools.jackson.databind.JsonNode) counts.get(attempt)).get("effects").asInt());
            assertEquals(false, ((tools.jackson.databind.JsonNode) MonitoringCollector.inside(
                    JSON.readTree("{\"mode\":\"summary\"}"), client, urls, base + "/simulator")).get("deduplicate").asBoolean());
        } finally {
            server.stop(0);
        }
    }

    private static void reply(HttpExchange exchange, int status, String body) throws java.io.IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (var output = exchange.getResponseBody()) { output.write(bytes); }
    }
}

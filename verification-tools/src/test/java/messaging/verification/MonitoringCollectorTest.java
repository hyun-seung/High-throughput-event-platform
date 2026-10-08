package messaging.verification;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MonitoringCollectorTest {
    @Test
    void staleOrFailedProbeDoesNotPublishOldLag() {
        var row = Map.<String, Object>of("topic", "message.received.v1", "partition", 0,
                "start", 0L, "end", 12L, "lag", 2L, "oldestUncommittedAgeSeconds", 3d);
        var snapshot = Map.<String, Object>of("partitions", List.of(row));
        for (boolean healthy : List.of(false, true)) {
            String stale = MonitoringCollector.exposition(snapshot, 100, healthy, healthy ? 146 : 101);
            assertTrue(stale.contains("platform_kafka_probe_up 0"));
            assertFalse(stale.contains("platform_kafka_committed_lag{"));
        }
        String fresh = MonitoringCollector.exposition(snapshot, 100, true, 110);
        assertTrue(fresh.contains("platform_kafka_probe_up 1"));
        assertTrue(fresh.contains("group=\"messaging-pre-send-manager\"} 2"));
        assertTrue(fresh.contains("platform_kafka_retained_records{topic=\"message.received.v1\",partition=\"0\"} 12"));
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

    private static void reply(HttpExchange exchange, int status, String body) throws java.io.IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (var output = exchange.getResponseBody()) { output.write(bytes); }
    }
}

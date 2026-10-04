package messaging.verification;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.file.StandardOpenOption.WRITE;

import static org.junit.jupiter.api.Assertions.*;

class MonitoringDemoTest {
    @Test
    void schedulesRequestsRepeatsEveryTenthKeyAndSendsOneForcedFailure(@TempDir Path root) throws Exception {
        var ids = new ConcurrentHashMap<String, String>();
        var nextId = new AtomicInteger();
        var normal = new AtomicInteger();
        var forced = new AtomicInteger();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/auth/token", exchange -> {
            assertEquals("POST", exchange.getRequestMethod());
            reply(exchange, "{\"data\":{\"accessToken\":\"fixture-token\"}}");
        });
        server.createContext("/api/v1/deliveries", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals("Bearer fixture-token", exchange.getRequestHeaders().getFirst("Authorization"));
            String key = exchange.getRequestHeaders().getFirst("Idempotency-Key");
            if (body.contains("forceFail")) forced.incrementAndGet();
            else normal.incrementAndGet();
            String id = ids.computeIfAbsent(key, ignored -> "delivery-" + nextId.incrementAndGet());
            reply(exchange, "{\"data\":{\"deliveryId\":\"" + id + "\"}}");
        });
        server.start();
        try (var client = HttpClient.newHttpClient()) {
            String base = "http://127.0.0.1:" + server.getAddress().getPort();
            List<Map<String, Object>> output = MonitoringDemo.execute(root, base, 10, 1, true, client);
            assertEquals(2, output.size());
            assertEquals(10, output.getFirst().get("requests"));
            assertEquals(9L, output.getFirst().get("uniqueIds"));
            assertEquals(true, output.getFirst().get("all202"));
            assertEquals(10, normal.get());
            assertEquals(1, forced.get());
            assertEquals(10, ids.size());
            assertTrue(ids.containsKey(output.getFirst().get("run") + "-8"));
            assertFalse(ids.containsKey(output.getFirst().get("run") + "-9"));
            assertEquals(true, output.get(1).get("intentionalProviderFailure"));
            assertEquals(202, output.get(1).get("status"));
            assertThrows(IllegalArgumentException.class, () -> MonitoringDemo.execute(root, base, 101, 1, false, client));
            try (var channel = FileChannel.open(root.resolve(".monitoring/benchmark.lock"), WRITE);
                 var lock = channel.lock()) {
                assertThrows(IllegalStateException.class,
                        () -> MonitoringDemo.execute(root, base, 1, 1, false, client));
            }
            assertTrue(Files.exists(root.resolve(".monitoring/benchmark.lock")));
        } finally {
            server.stop(0);
        }
    }

    private static void reply(HttpExchange exchange, String body) throws java.io.IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(202, bytes.length);
        try (var output = exchange.getResponseBody()) { output.write(bytes); }
    }
}

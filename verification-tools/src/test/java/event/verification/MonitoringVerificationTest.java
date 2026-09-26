package event.verification;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MonitoringVerificationTest {
    private static final String DASHBOARD = """
            {"uid":"delivery-test","panels":[
              {"id":1,"title":"Rate","type":"timeseries","datasource":{"uid":"platform-prometheus"},
               "targets":[{"expr":"rate(x[$__rate_interval])"}]},
              {"id":2,"title":"Audit","type":"logs","datasource":{"uid":"platform-loki"},
               "targets":[{"expr":"{tenant=\\\"$tenant\\\"}"}]}
            ]}
            """;

    @Test
    void checksDatasourceQueriesProvisioningAndAuditWithoutChangingRuntime(@TempDir Path root) throws Exception {
        Path boards = root.resolve("monitoring/grafana/dashboards");
        Files.createDirectories(boards);
        Files.writeString(boards.resolve("test.json"), DASHBOARD);
        var mismatch = new AtomicBoolean();
        var sensitive = new AtomicBoolean();
        var panelQueries = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> reply(exchange, mismatch, sensitive, panelQueries));
        server.start();
        try (var client = HttpClient.newHttpClient()) {
            String base = "http://127.0.0.1:" + server.getAddress().getPort();
            var passed = MonitoringVerification.verify(root, base, base, client);
            assertEquals(true, passed.get("pass"));
            assertEquals(2, ((List<?>) passed.get("queries")).size());
            assertEquals(1, passed.get("auditLogsInspected"));
            assertEquals(1, panelQueries.get());
            assertFalse(Files.exists(root.resolve(".monitoring/verification.json")));

            mismatch.set(true);
            sensitive.set(true);
            var failed = MonitoringVerification.verify(root, base, base, client);
            assertEquals(false, failed.get("pass"));
            @SuppressWarnings("unchecked") var errors = (List<Map<String, Object>>) failed.get("errors");
            assertTrue(errors.stream().anyMatch(error -> "provisioning mismatch".equals(error.get("error"))));
            assertTrue(errors.stream().anyMatch(error -> "Unexpected sensitive field".equals(error.get("logs"))));
        } finally {
            server.stop(0);
        }
    }

    private static void reply(HttpExchange exchange, AtomicBoolean mismatch, AtomicBoolean sensitive,
                              AtomicInteger panelQueries) throws java.io.IOException {
        String path = exchange.getRequestURI().getPath();
        String query = exchange.getRequestURI().getRawQuery();
        String body;
        if (path.equals("/api/v1/targets")) {
            body = """
                    {"data":{"activeTargets":[{"labels":{"job":"api"},"health":"up","lastError":""}]}}
                    """;
        } else if (path.startsWith("/api/dashboards/uid/")) {
            body = "{\"dashboard\":" + (mismatch.get() ? DASHBOARD.replace("Rate", "Changed") : DASHBOARD) + "}";
        } else if (path.equals("/api/v1/query")) {
            if (query.contains("rate%28x%5B2m%5D%29")) panelQueries.incrementAndGet();
            body = "{\"status\":\"success\",\"data\":{\"result\":[{}]}}";
        } else if (path.endsWith("/query_range") && query != null && query.contains("limit=1000")) {
            String record = sensitive.get() ? "{\"logger_name\":\"delivery.audit\",\"password\":\"secret\"}"
                    : "{\"logger_name\":\"delivery.audit\"}";
            body = "{\"data\":{\"result\":[{\"values\":[[\"1\"," + quote(record) + "]]}]}}";
        } else if (path.endsWith("/query_range")) {
            body = "{\"status\":\"success\",\"data\":{\"result\":[{}]}}";
        } else {
            body = "{}";
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (var output = exchange.getResponseBody()) { output.write(bytes); }
    }

    private static String quote(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}

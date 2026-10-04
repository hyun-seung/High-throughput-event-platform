package messaging.verification;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Read-only Grafana, Prometheus, and audit-log verification. */
final class MonitoringVerification {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final DateTimeFormatter UTC_MICROS = DateTimeFormatter
            .ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS'+00:00'").withZone(ZoneOffset.UTC);
    private static final List<String> SENSITIVE = List.of("payload", "password", "accessToken", "authorization", "Idempotency-Key");
    private static final Map<String, String> BUSINESS_QUERIES = Map.of(
            "persisted_acceptances", "sum(delivery_outcomes_total{application=\"dispatch-worker\",outcome=\"dispatch_accepted\"}) > 0",
            "fresh_kafka_probe", "platform_kafka_probe_up == 1",
            "drained_backlog", "sum(platform_kafka_committed_lag) == 0");

    private MonitoringVerification() {}

    static void run(Path root, String grafana, String prometheus) throws Exception {
        var proxy = new ProxySelector() {
            @Override public List<Proxy> select(URI uri) { return List.of(Proxy.NO_PROXY); }
            @Override public void connectFailed(URI uri, java.net.SocketAddress address, IOException failure) {}
        };
        try (var client = HttpClient.newBuilder().proxy(proxy).connectTimeout(Duration.ofSeconds(30)).build()) {
            var report = verify(root.toAbsolutePath(), grafana, prometheus, client);
            Path output = root.toAbsolutePath().resolve(".monitoring/verification.json");
            Files.createDirectories(output.getParent());
            Files.writeString(output, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(report) + "\n");
            var summary = new LinkedHashMap<String, Object>();
            summary.put("pass", report.get("pass"));
            summary.put("queries", ((List<?>) report.get("queries")).size());
            summary.put("auditLogs", report.get("auditLogsInspected"));
            summary.put("errors", report.get("errors"));
            System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(summary));
            if (!Boolean.TRUE.equals(report.get("pass"))) System.exit(1);
        }
    }

    static Map<String, Object> verify(Path root, String grafana, String prometheus, HttpClient client) throws Exception {
        var errors = new ArrayList<Map<String, Object>>();
        var queries = new ArrayList<Map<String, Object>>();
        var targets = get(client, prometheus + "/api/v1/targets").get("data").get("activeTargets");
        var targetSummary = new ArrayList<Map<String, String>>();
        for (var target : targets) {
            String job = target.get("labels").get("job").asText();
            String health = target.get("health").asText();
            targetSummary.add(Map.of("job", job, "health", health));
            if (!health.equals("up")) errors.add(Map.of("target", job, "error", target.get("lastError").asText()));
        }

        try (var files = Files.list(root.resolve("monitoring/grafana/dashboards"))) {
            for (Path path : files.filter(file -> file.getFileName().toString().endsWith(".json"))
                    .sorted(Comparator.comparing(file -> file.getFileName().toString())).toList()) {
                var local = JSON.readTree(Files.readString(path));
                String uid = local.get("uid").asText();
                var remote = get(client, grafana + "/api/dashboards/uid/" + uid).get("dashboard");
                if (!panels(local).equals(panels(remote))) errors.add(Map.of("dashboard", uid, "error", "provisioning mismatch"));
                for (var panel : local.get("panels")) {
                    var panelTargets = panel.get("targets");
                    if (panelTargets == null) continue;
                    for (var target : panelTargets) {
                        String query = target.get("expr").asText().replace("$__rate_interval", "2m")
                                .replace("$__range", "15m").replace("$tenant", "1").replace("$delivery", "");
                        boolean loki = panel.get("datasource").get("uid").asText().equals("platform-loki");
                        String base = loki ? grafana + "/api/datasources/proxy/uid/platform-loki/loki/api/v1/"
                                : prometheus + "/api/v1/";
                        String endpoint = panel.get("type").asText().equals("logs") ? "query_range" : "query";
                        String url = base + endpoint + "?query=" + encoded(query) + (loki ? "&limit=20" : "");
                        try {
                            var result = get(client, url);
                            if (!"success".equals(result.path("status").asText())) throw new IllegalStateException(result.toString());
                            var observation = new LinkedHashMap<String, Object>();
                            observation.put("dashboard", uid);
                            observation.put("panel", panel.get("title").asText());
                            observation.put("series", result.get("data").get("result").size());
                            observation.put("warnings", result.has("warnings") ? result.get("warnings") : List.of());
                            queries.add(observation);
                        } catch (Exception failure) {
                            errors.add(Map.of("panel", panel.get("title").asText(), "query", query,
                                    "error", String.valueOf(failure.getMessage())));
                        }
                    }
                }
            }
        }

        var logs = get(client, grafana + "/api/datasources/proxy/uid/platform-loki/loki/api/v1/query_range?query="
                + encoded("{service=~\"api|ingress|dispatch\"}") + "&limit=1000");
        int auditLogs = 0;
        for (var stream : logs.get("data").get("result")) {
            for (var entry : stream.get("values")) {
                auditLogs++;
                var record = JSON.readTree(entry.get(1).asText());
                if (!"delivery.audit".equals(record.path("logger_name").asText())) {
                    errors.add(Map.of("logs", "Unexpected non-audit logger"));
                }
                if (SENSITIVE.stream().anyMatch(record::has)) errors.add(Map.of("logs", "Unexpected sensitive field"));
            }
        }
        if (auditLogs == 0) errors.add(Map.of("logs", "No audit logs; run demo after startup"));

        for (String name : List.of("persisted_acceptances", "fresh_kafka_probe", "drained_backlog")) {
            var observed = get(client, prometheus + "/api/v1/query?query=" + encoded(BUSINESS_QUERIES.get(name)));
            if (observed.get("data").get("result").isEmpty()) {
                errors.add(Map.of("businessCheck", name, "error", "Expected observation absent"));
            }
        }
        for (String service : List.of("api", "ingress", "dispatch")) {
            var observed = get(client, grafana + "/api/datasources/proxy/uid/platform-loki/loki/api/v1/query_range?query="
                    + encoded("{service=\"" + service + "\"}") + "&limit=1");
            if (observed.get("data").get("result").isEmpty()) errors.add(Map.of("logs", service + " audit events absent"));
        }

        var report = new LinkedHashMap<String, Object>();
        report.put("checkedAt", UTC_MICROS.format(Instant.now()));
        report.put("targets", targetSummary);
        report.put("queries", queries);
        report.put("auditLogsInspected", auditLogs);
        report.put("errors", errors);
        report.put("pass", errors.isEmpty());
        return report;
    }

    private record PanelSpec(String title, JsonNode targets, JsonNode transformations) {}

    private static Map<Integer, PanelSpec> panels(JsonNode dashboard) {
        var result = new LinkedHashMap<Integer, PanelSpec>();
        for (var panel : dashboard.get("panels")) {
            result.put(panel.get("id").asInt(), new PanelSpec(panel.get("title").asText(),
                    panel.get("targets"), panel.get("transformations")));
        }
        return result;
    }

    private static JsonNode get(HttpClient client, String url) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(30)).GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("HTTP " + response.statusCode() + " from " + url);
        }
        return JSON.readTree(response.body());
    }

    private static String encoded(String value) { return URLEncoder.encode(value, StandardCharsets.UTF_8); }
}

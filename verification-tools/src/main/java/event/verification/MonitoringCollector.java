package event.verification;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
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
import java.util.concurrent.Executors;

/** Read-only Kafka observations and authenticated API metrics relay. */
final class MonitoringCollector {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String REQUESTED = "delivery.requested.v1";
    private static final String DISPATCH = "delivery.dispatch-requested.v1";
    private static final String DLT = "delivery.requested.dlt.v1";
    private static final List<String> TOPICS = List.of(REQUESTED, DISPATCH, DLT);
    private static final Map<String, String> GROUPS = Map.of(REQUESTED, "delivery-ingress-worker", DISPATCH, "delivery-dispatch-worker");
    private static final String API = "http://api:8080";
    private static final String SIMULATOR = "http://simulator:19090";
    private static final Map<String, String> SCRAPES = Map.of("api", "http://api:19080", "ingress", "http://ingress:19081",
            "dispatch", "http://dispatch:19082", "simulator", SIMULATOR);

    private MonitoringCollector() {}

    static void serve() throws Exception {
        var state = new State();
        Thread probe = new Thread(() -> kafkaLoop(state), "kafka-probe");
        probe.setDaemon(true);
        probe.start();
        var server = HttpServer.create(new InetSocketAddress("0.0.0.0", 9800), 0);
        server.createContext("/metrics/api", exchange -> respond(exchange, "/metrics/api", () -> state.apiMetrics()));
        server.createContext("/metrics", exchange -> respond(exchange, "/metrics", () -> exposition(state.snapshot, state.lastSuccess, state.healthy,
                System.currentTimeMillis() / 1000d).getBytes(StandardCharsets.UTF_8)));
        server.createContext("/health", exchange -> respond(exchange, "/health", () -> "collector running\n".getBytes(StandardCharsets.UTF_8)));
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("Collector listening on 9800");
    }

    static void inside() throws Exception {
        JsonNode input = JSON.readTree(System.in);
        try (var http = httpClient()) {
            System.out.println(JSON.writeValueAsString(inside(input, http, SCRAPES, SIMULATOR)));
        }
    }

    static Object inside(JsonNode input, HttpClient http, Map<String, String> scrapes, String simulator) throws Exception {
        String mode = input.path("mode").asText();
            Object result;
            if (mode.equals("metrics")) {
                String token = input.get("token").asText();
                var output = new LinkedHashMap<String, String>();
                try (var pool = Executors.newFixedThreadPool(4)) {
                    var futures = new LinkedHashMap<String, java.util.concurrent.Future<String>>();
                    for (String app : List.of("api", "ingress", "dispatch", "simulator")) {
                        futures.put(app, pool.submit(() -> {
                            var response = request(http, scrapes.get(app) + "/actuator/prometheus",
                                    app.equals("api") ? token : null, null);
                            if (response.statusCode() != 200) throw new IOException(app + " metrics HTTP " + response.statusCode());
                            return new String(response.body(), StandardCharsets.UTF_8);
                        }));
                    }
                    for (var entry : futures.entrySet()) output.put(entry.getKey(), entry.getValue().get());
                }
                result = output;
            } else if (mode.equals("counts")) {
                var output = new LinkedHashMap<String, Object>();
                try (var pool = Executors.newFixedThreadPool(8)) {
                    var futures = new LinkedHashMap<String, java.util.concurrent.Future<Object>>();
                    for (JsonNode delivery : input.get("deliveries")) {
                        String attempt = UUID.nameUUIDFromBytes(("attempt:" + delivery.asText() + ":mock-provider:1:1")
                                .getBytes(StandardCharsets.UTF_8)).toString();
                        futures.put(attempt, pool.submit(() -> {
                            var response = request(http, simulator + "/actuator/simulator/" + attempt, null, null);
                            if (response.statusCode() == 404) return Map.of("calls", 0, "effects", 0);
                            if (response.statusCode() == 200) return JSON.readTree(response.body());
                            throw new IOException("Simulator counts HTTP " + response.statusCode());
                        }));
                    }
                    for (var entry : futures.entrySet()) output.put(entry.getKey(), entry.getValue().get());
                }
                result = output;
            } else if (mode.equals("summary")) {
                var response = request(http, simulator + "/actuator/simulator", null, null);
                if (response.statusCode() != 200) throw new IOException("Simulator summary HTTP " + response.statusCode());
                result = JSON.readTree(response.body());
            } else throw new IllegalArgumentException("Unknown collector mode: " + mode);
            return result;
    }

    private interface Body { byte[] read() throws Exception; }

    private static void respond(HttpExchange exchange, String path, Body body) throws IOException {
        try {
            if (!exchange.getRequestURI().getPath().equals(path)) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }
            if (!exchange.getRequestMethod().equals("GET")) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            byte[] bytes = body.read();
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, bytes.length);
            try (var output = exchange.getResponseBody()) { output.write(bytes); }
        } catch (Exception failure) {
            exchange.sendResponseHeaders(503, -1);
        } finally {
            exchange.close();
        }
    }

    private static void kafkaLoop(State state) {
        KafkaProbe probe = null;
        while (true) {
            long started = System.nanoTime();
            try {
                if (probe == null) probe = new KafkaProbe(System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"));
                state.snapshot = probe.snapshot();
                state.lastSuccess = System.currentTimeMillis() / 1000d;
                state.healthy = true;
            } catch (Exception failure) {
                state.healthy = false;
                if (probe != null) probe.close();
                probe = null;
            }
            long elapsed = Duration.ofNanos(System.nanoTime() - started).toMillis();
            try { Thread.sleep(Math.max(1000, 10000 - elapsed)); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
        }
    }

    static String exposition(Map<String, Object> snapshot, double lastSuccess, boolean healthy, double now) {
        boolean fresh = healthy && now - lastSuccess < 45;
        var lines = new ArrayList<String>();
        lines.add("# TYPE platform_kafka_probe_up gauge");
        lines.add("platform_kafka_probe_up " + (fresh ? 1 : 0));
        lines.add("# TYPE platform_kafka_probe_last_success_timestamp_seconds gauge");
        lines.add("platform_kafka_probe_last_success_timestamp_seconds " + lastSuccess);
        if (!fresh || snapshot == null) return String.join("\n", lines) + "\n";
        @SuppressWarnings("unchecked") var rows = (List<Map<String, Object>>) snapshot.get("partitions");
        for (var row : rows) {
            String topic = (String) row.get("topic");
            String labels = "topic=\"" + topic + "\",partition=\"" + row.get("partition") + "\"";
            lines.add("platform_kafka_log_end_offset{" + labels + "} " + row.get("end"));
            lines.add("platform_kafka_retained_records{" + labels + "} "
                    + (((Number) row.get("end")).longValue() - ((Number) row.get("start")).longValue()));
            if (row.get("lag") != null) {
                String group = GROUPS.get(topic);
                lines.add("platform_kafka_committed_lag{" + labels + ",group=\"" + group + "\"} " + row.get("lag"));
                lines.add("platform_kafka_oldest_uncommitted_age_seconds{" + labels + ",group=\"" + group + "\"} "
                        + (row.get("oldestUncommittedAgeSeconds") == null ? 0 : row.get("oldestUncommittedAgeSeconds")));
            }
        }
        return String.join("\n", lines) + "\n";
    }

    private static HttpClient httpClient() {
        var proxy = new ProxySelector() {
            @Override public List<Proxy> select(URI uri) { return List.of(Proxy.NO_PROXY); }
            @Override public void connectFailed(URI uri, java.net.SocketAddress address, IOException error) {}
        };
        return HttpClient.newBuilder().proxy(proxy).connectTimeout(Duration.ofSeconds(5)).build();
    }

    private static HttpResponse<byte[]> request(HttpClient http, String url, String token, Object payload) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5));
        if (token != null) builder.header("Authorization", "Bearer " + token);
        if (payload == null) builder.GET();
        else builder.header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(payload)));
        var response = http.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() >= 400 && response.statusCode() != 401 && response.statusCode() != 404)
            throw new IOException("HTTP " + response.statusCode() + " from " + url);
        return response;
    }

    static final class State {
        volatile Map<String, Object> snapshot;
        volatile double lastSuccess;
        volatile boolean healthy;
        private String token;
        private final HttpClient http;
        private final String api;
        private final String metrics;

        State() { this(API, "http://api:19080", httpClient()); }

        State(String api, String metrics, HttpClient http) {
            this.api = api;
            this.metrics = metrics;
            this.http = http;
        }

        synchronized byte[] apiMetrics() throws Exception {
            for (int attempt = 0; attempt < 2; attempt++) {
                if (token == null) {
                    var auth = request(http, api + "/api/v1/auth/token", null,
                            Map.of("username", "local-user", "password", "local-password"));
                    if (auth.statusCode() != 200) throw new IOException("API authentication HTTP " + auth.statusCode());
                    token = JSON.readTree(auth.body()).get("data").get("accessToken").asText();
                }
                var response = request(http, metrics + "/actuator/prometheus", token, null);
                if (response.statusCode() == 200) return response.body();
                if (response.statusCode() != 401 || attempt == 1) throw new IOException("API metrics HTTP " + response.statusCode());
                token = null;
            }
            throw new IOException("Authentication unavailable");
        }
    }

    private static final class KafkaProbe implements AutoCloseable {
        private final KafkaConsumer<byte[], byte[]> reader;
        private final Map<String, KafkaConsumer<byte[], byte[]>> groups = new LinkedHashMap<>();
        private final Map<String, List<TopicPartition>> partitions = new LinkedHashMap<>();

        KafkaProbe(String bootstrap) {
            reader = consumer(bootstrap, "poc-read-only-" + UUID.randomUUID());
            try {
                GROUPS.forEach((topic, group) -> groups.put(topic, consumer(bootstrap, group)));
                for (String topic : TOPICS) {
                    var metadata = reader.partitionsFor(topic, Duration.ofSeconds(10));
                    if (metadata == null || metadata.isEmpty()) throw new IllegalStateException("Missing topic " + topic);
                    partitions.put(topic, metadata.stream().map(partition -> new TopicPartition(topic, partition.partition()))
                            .sorted(java.util.Comparator.comparingInt(TopicPartition::partition)).toList());
                }
            } catch (Exception failure) {
                close();
                throw failure;
            }
        }

        Map<String, Object> snapshot() {
            double now = System.currentTimeMillis() / 1000d;
            var rows = new ArrayList<Map<String, Object>>();
            long totalLag = 0;
            double oldest = 0;
            for (String topic : TOPICS) {
                var parts = partitions.get(topic);
                var beginnings = reader.beginningOffsets(parts, Duration.ofSeconds(5));
                var ends = reader.endOffsets(parts, Duration.ofSeconds(5));
                var commits = groups.containsKey(topic) ? groups.get(topic).committed(new java.util.HashSet<>(parts), Duration.ofSeconds(5)) : Map.<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata>of();
                for (var part : parts) {
                    long start = beginnings.get(part), end = ends.get(part);
                    var commit = commits.get(part);
                    Long committed = groups.containsKey(topic) ? (commit == null ? -1001L : commit.offset()) : null;
                    Long effective = committed == null ? null : committed < 0 ? start : committed;
                    if (effective != null && (effective < start || effective > end))
                        throw new IllegalStateException("Committed offset outside retained range; do not report zero lag");
                    Long lag = effective == null ? null : end - effective;
                    Double age = null;
                    if (lag != null && lag > 0) {
                        reader.assign(List.of(part));
                        reader.seek(part, effective);
                        var records = reader.poll(Duration.ofSeconds(3)).records(part);
                        if (records.isEmpty() || records.getFirst().offset() != effective)
                            throw new IllegalStateException("Cannot inspect oldest uncommitted record");
                        long timestamp = records.getFirst().timestamp();
                        if (timestamp >= 0) age = Math.max(0, now - timestamp / 1000d);
                    }
                    var row = new LinkedHashMap<String, Object>();
                    row.put("topic", topic);
                    row.put("partition", part.partition());
                    row.put("start", start);
                    row.put("end", end);
                    row.put("committed", committed);
                    row.put("lag", lag);
                    row.put("oldestUncommittedAgeSeconds", age);
                    rows.add(row);
                    if (lag != null) totalLag += lag;
                    if (age != null) oldest = Math.max(oldest, age);
                }
            }
            return Map.of("time", now, "partitions", rows, "lag", totalLag, "oldestUncommittedAgeSeconds", oldest);
        }

        private static KafkaConsumer<byte[], byte[]> consumer(String bootstrap, String group) {
            var config = new LinkedHashMap<String, Object>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
            config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            config.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
            config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            return new KafkaConsumer<>(config);
        }

        @Override public void close() {
            reader.close(Duration.ofSeconds(1));
            groups.values().forEach(consumer -> consumer.close(Duration.ofSeconds(1)));
        }
    }
}

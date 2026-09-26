package event.verification;

import tools.jackson.databind.json.JsonMapper;

import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

final class MonitoringDemo {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String BASE = "http://127.0.0.1:38080";

    private MonitoringDemo() {}

    static void run(String[] args) throws Exception {
        int rate = 20;
        int seconds = 30;
        boolean errors = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--rate" -> rate = Integer.parseInt(args[++i]);
                case "--seconds" -> seconds = Integer.parseInt(args[++i]);
                case "--errors" -> errors = true;
                default -> throw new IllegalArgumentException("Usage: demo [--rate 1..100] [--seconds 1..120] [--errors]");
            }
        }
        validate(rate, seconds);
        var noProxy = new ProxySelector() {
            @Override public List<java.net.Proxy> select(URI uri) { return List.of(java.net.Proxy.NO_PROXY); }
            @Override public void connectFailed(URI uri, java.net.SocketAddress address, java.io.IOException error) {}
        };
        try (var client = HttpClient.newBuilder().proxy(noProxy).connectTimeout(Duration.ofSeconds(15)).build()) {
            for (var result : execute(Path.of(""), BASE, rate, seconds, errors, client)) {
                System.out.println(JSON.writeValueAsString(result));
            }
        }
    }

    static List<Map<String, Object>> execute(Path root, String base, int rate, int seconds,
                                              boolean errors, HttpClient client) throws Exception {
        validate(rate, seconds);
        Path directory = root.resolve(".monitoring");
        Files.createDirectories(directory);
        try (var channel = FileChannel.open(directory.resolve("benchmark.lock"), CREATE, WRITE)) {
            java.nio.channels.FileLock lock;
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException overlap) {
                throw new IllegalStateException("Monitoring benchmark is already running", overlap);
            }
            if (lock == null) throw new IllegalStateException("Monitoring benchmark is already running");
            try (lock) {
                String token = request(client, base, "/api/v1/auth/token",
                        Map.of("username", "local-user", "password", "local-password"), null, null)
                        .body().get("data").get("accessToken").asText();
                String run = "monitor-demo-" + UUID.randomUUID().toString().replace("-", "");
                long started = System.nanoTime();
                var futures = new ArrayList<Future<Delivery>>();
                try (var pool = Executors.newFixedThreadPool(20)) {
                    for (int i = 0; i < rate * seconds; i++) {
                        long delay = started + (long) (i * 1_000_000_000d / rate) - System.nanoTime();
                        if (delay > 0) LockSupport.parkNanos(delay);
                        final int index = i;
                        futures.add(pool.submit(() -> {
                            String key = run + "-" + (index % 10 == 9 ? index - 1 : index);
                            var response = request(client, base, "/api/v1/deliveries",
                                    Map.of("deliveryType", "EMAIL", "payload", Map.of("message", "monitoring demo")), token, key);
                            return new Delivery(response.status(), response.body().get("data").get("deliveryId").asText());
                        }));
                    }
                    var results = new ArrayList<Delivery>();
                    for (var future : futures) results.add(future.get());
                    var summary = new LinkedHashMap<String, Object>();
                    summary.put("run", run);
                    summary.put("requests", results.size());
                    summary.put("uniqueIds", results.stream().map(Delivery::id).distinct().count());
                    summary.put("all202", results.stream().allMatch(result -> result.status() == 202));
                    summary.put("exampleDeliveryId", results.getFirst().id());
                    var output = new ArrayList<Map<String, Object>>();
                    output.add(summary);
                    if (errors) {
                        var response = request(client, base, "/api/v1/deliveries",
                                Map.of("deliveryType", "EMAIL", "payload", Map.of("message", "explicit failure demo", "forceFail", true)),
                                token, run + "-forced-failure");
                        var failure = new LinkedHashMap<String, Object>();
                        failure.put("intentionalProviderFailure", true);
                        failure.put("status", response.status());
                        failure.put("deliveryId", response.body().get("data").get("deliveryId").asText());
                        output.add(failure);
                    }
                    return output;
                }
            }
        }
    }

    private static void validate(int rate, int seconds) {
        if (rate < 1 || rate > 100 || seconds < 1 || seconds > 120)
            throw new IllegalArgumentException("rate 1..100 and seconds 1..120 required");
    }

    private static Response request(HttpClient client, String base, String path, Object payload,
                                    String token, String key) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(15)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(payload), StandardCharsets.UTF_8));
        if (token != null) builder.header("Authorization", "Bearer " + token);
        if (key != null) builder.header("Idempotency-Key", key);
        var response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() < 200 || response.statusCode() >= 300)
            throw new IllegalStateException(path + " returned HTTP " + response.statusCode());
        return new Response(response.statusCode(), JSON.readTree(response.body()));
    }

    private record Response(int status, tools.jackson.databind.JsonNode body) {}
    private record Delivery(int status, String id) {}
}

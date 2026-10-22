package messaging.verification;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import messaging.common.dynamodb.DynamoDbAutoConfiguration;
import messaging.common.dynamodb.DynamoDbProperties;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Local-only synthetic SDK workloads. Results are neither AP nor production DynamoDB capacity. */
final class DynamoDbBenchmark {
    private static final class Registry extends SimpleMeterRegistry implements AutoCloseable { }
    enum Profile {
        CONDITIONAL_PUT(1), GET_UPDATE(2), RESULT_CYCLE(6);
        final int logicalCalls;
        Profile(int logicalCalls) { this.logicalCalls = logicalCalls; }
    }
    record Load(int workers, int connections) { }
    record Settings(URI endpoint, Path output, List<Load> loads, List<Profile> profiles,
                    int seconds, int warmupSeconds, int rounds) {
        static Settings parse(String[] args) {
            var values = new HashMap<String, String>();
            var allowed = Set.of("--endpoint", "--output", "--cases", "--profiles", "--seconds", "--warmup-seconds", "--rounds");
            if (args.length % 2 != 0) throw new IllegalArgumentException("Options require values");
            for (int i = 0; i < args.length; i += 2) {
                if (!allowed.contains(args[i]) || values.put(args[i], args[i + 1]) != null) {
                    throw new IllegalArgumentException("Unknown or duplicate option: " + args[i]);
                }
            }
            URI endpoint = URI.create(Objects.requireNonNull(values.get("--endpoint"), "Explicit local endpoint required"));
            if (!"http".equals(endpoint.getScheme()) || endpoint.getHost() == null
                    || !Set.of("localhost", "127.0.0.1", "[::1]", "dynamodb-local").contains(endpoint.getHost())
                    || endpoint.getUserInfo() != null || endpoint.getQuery() != null || endpoint.getFragment() != null
                    || !(endpoint.getPath().isEmpty() || "/".equals(endpoint.getPath()))) {
                throw new IllegalArgumentException("Only an explicit loopback or monitoring DynamoDB Local endpoint is allowed");
            }
            var loads = new ArrayList<Load>();
            for (String entry : values.getOrDefault("--cases", "1:64,8:64,64:64,128:64,128:128").split(",")) {
                String[] pair = entry.split(":");
                if (pair.length != 2) throw new IllegalArgumentException("Cases are workers:connections");
                int workers = Integer.parseInt(pair[0]), connections = Integer.parseInt(pair[1]);
                if (workers < 1 || workers > 128 || connections < 1 || connections > 256) throw new IllegalArgumentException("Invalid worker/pool limit");
                loads.add(new Load(workers, connections));
            }
            var profiles = Arrays.stream(values.getOrDefault("--profiles", "conditional-put,get-update,result-cycle").split(","))
                    .map(value -> Profile.valueOf(value.toUpperCase(Locale.ROOT).replace('-', '_'))).toList();
            int seconds = Integer.parseInt(values.getOrDefault("--seconds", "3"));
            int warmup = Integer.parseInt(values.getOrDefault("--warmup-seconds", "1"));
            int rounds = Integer.parseInt(values.getOrDefault("--rounds", "1"));
            if (seconds < 1 || seconds > 10 || warmup < 0 || warmup > 3 || rounds < 1 || rounds > 3
                    || loads.size() > 10 || profiles.isEmpty() || profiles.size() > 3
                    || new HashSet<>(profiles).size() != profiles.size()) throw new IllegalArgumentException("Invalid bounded benchmark duration/matrix");
            return new Settings(endpoint, Path.of(Objects.requireNonNull(values.get("--output"), "Output required")),
                    List.copyOf(loads), profiles, seconds, warmup, rounds);
        }
    }

    static void run(String[] args) throws Exception {
        var settings = Settings.parse(args);
        var mapper = JsonMapper.builder().build();
        var report = new LinkedHashMap<String, Object>();
        report.put("scope", "synthetic local SDK workload; AP/Kafka/SQL/business capacity not measured");
        report.put("settings", settings);
        report.put("startedAt", Instant.now());
        report.put("javaVersion", System.getProperty("java.version"));
        report.put("availableProcessors", Runtime.getRuntime().availableProcessors());
        report.put("maxHeapBytes", Runtime.getRuntime().maxMemory());
        String table = "platform_sdk_bench_" + UUID.randomUUID().toString().replace("-", "");
        report.put("ownedTable", table);
        var rows = new ArrayList<Map<String, Object>>();
        report.put("phases", rows);
        save(settings.output(), mapper, report);
        boolean failed = false;
        try (var metrics = new Registry(); var admin = client(settings.endpoint(), 64, metrics)) {
            try {
                withOwnedTable(admin, table, () -> {
                    for (int round = 0; round < settings.rounds(); round++) {
                        var loads = new ArrayList<>(settings.loads());
                        if (round % 2 == 1) Collections.reverse(loads);
                        for (var load : loads) {
                            for (var profile : settings.profiles()) {
                                try (var registry = new Registry(); var db = client(settings.endpoint(), load.connections(), registry)) {
                                    String prefix = UUID.randomUUID().toString();
                                    seed(db, table, prefix, load.workers());
                                    if (settings.warmupSeconds() > 0) {
                                        var warmup = phase(db, table, prefix + "-warm", prefix, load.workers(), profile, settings.warmupSeconds());
                                        if ((int) warmup.get("errors") > 0 || (boolean) warmup.get("recordLimitReached")) throw new IllegalStateException("Warmup failed or capped");
                                    }
                                    var before = snapshot(registry);
                                    var result = phase(db, table, prefix + "-measure", prefix, load.workers(), profile, settings.seconds());
                                    result.put("round", round + 1);
                                    result.put("workers", load.workers());
                                    result.put("connections", load.connections());
                                    result.put("sdk", timers(registry, before));
                                    rows.add(result);
                                    save(settings.output(), mapper, report);
                                    System.out.println(mapper.writeValueAsString(result));
                                    if ((int) result.get("errors") > 0 || (boolean) result.get("recordLimitReached")) throw new IllegalStateException("Measured phase failed or capped");
                                }
                            }
                        }
                    }
                    return null;
                });
            } catch (Exception error) {
                report.put("failure", error.getClass().getSimpleName() + ": " + error.getMessage());
                failed = true;
            }
        } finally {
            report.put("finishedAt", Instant.now());
            report.put("ownedTableRemoved", tableAbsent(settings.endpoint(), table));
            report.put("pass", !failed && !rows.isEmpty() && rows.stream().allMatch(row -> (int) row.get("errors") == 0)
                    && Boolean.TRUE.equals(report.get("ownedTableRemoved")));
            save(settings.output(), mapper, report);
        }
        if (!Boolean.TRUE.equals(report.get("pass"))) throw new IllegalStateException("Benchmark failed; inspect " + settings.output());
    }

    private static DynamoDbClient client(URI endpoint, int connections, SimpleMeterRegistry registry) {
        var properties = new DynamoDbProperties();
        properties.setEndpoint(endpoint.toString());
        properties.setMaxConnections(connections);
        var beans = new StaticListableBeanFactory();
        beans.addBean("metrics", registry);
        return new DynamoDbAutoConfiguration().dynamoDbClient(properties, beans.getBeanProvider(io.micrometer.core.instrument.MeterRegistry.class));
    }

    static <T> T withOwnedTable(DynamoDbClient db, String table, Callable<T> action) throws Exception {
        if (!table.matches("platform_sdk_bench_[0-9a-f]{32}")) throw new IllegalArgumentException("Not a generated benchmark table");
        // If creation fails (including a name collision), no existing table is deleted.
        db.createTable(CreateTableRequest.builder().tableName(table).billingMode(BillingMode.PAY_PER_REQUEST)
                .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build()).build());
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (db.describeTable(DescribeTableRequest.builder().tableName(table).build()).table().tableStatus() != TableStatus.ACTIVE) {
                if (System.nanoTime() >= deadline) throw new IllegalStateException("Benchmark table not active");
                Thread.sleep(100);
            }
            return action.call();
        } finally {
            db.deleteTable(DeleteTableRequest.builder().tableName(table).build());
        }
    }

    private static boolean tableAbsent(URI endpoint, String table) {
        try (var registry = new Registry(); var db = client(endpoint, 64, registry)) {
            db.describeTable(DescribeTableRequest.builder().tableName(table).build());
            return false;
        } catch (ResourceNotFoundException absent) {
            return true;
        } catch (Exception unknown) {
            return false;
        }
    }

    private static void seed(DynamoDbClient db, String table, String prefix, int workers) {
        for (int worker = 0; worker < workers; worker++) {
            db.putItem(PutItemRequest.builder().tableName(table).item(item(prefix + "-origin-" + worker)).build());
            db.putItem(PutItemRequest.builder().tableName(table).item(item(prefix + "-step-" + worker)).build());
        }
    }

    static void execute(DynamoDbClient db, String table, Profile profile, String record, String origin, String step) {
        if (profile == Profile.CONDITIONAL_PUT || profile == Profile.RESULT_CYCLE) {
            db.putItem(PutItemRequest.builder().tableName(table).item(item(record)).conditionExpression("attribute_not_exists(pk)").build());
            if (profile == Profile.CONDITIONAL_PUT) return;
        }
        db.getItem(GetItemRequest.builder().tableName(table).key(key(origin)).consistentRead(true).build());
        if (profile == Profile.RESULT_CYCLE) {
            db.getItem(GetItemRequest.builder().tableName(table).key(key(step)).consistentRead(true).build());
        }
        db.updateItem(UpdateItemRequest.builder().tableName(table).key(key(origin))
                .conditionExpression("attribute_exists(pk)").updateExpression("SET record_id = :id")
                .expressionAttributeValues(Map.of(":id", AttributeValue.fromS(record))).build());
        if (profile == Profile.RESULT_CYCLE) {
            db.updateItem(UpdateItemRequest.builder().tableName(table).key(key(origin))
                    .conditionExpression("record_id = :id").updateExpression("SET ack_id = :id")
                    .expressionAttributeValues(Map.of(":id", AttributeValue.fromS(record))).build());
            db.updateItem(UpdateItemRequest.builder().tableName(table).key(key(record))
                    .conditionExpression("attribute_exists(pk)").updateExpression("SET #status = :done")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(":done", AttributeValue.fromS("DONE"))).build());
        }
    }

    private record Samples(List<Long> durations, int errors, int withinWindow, Map<String, Integer> failures, boolean capped) { }

    private static Map<String, Object> phase(DynamoDbClient db, String table, String recordsPrefix, String seedPrefix,
                                            int workers, Profile profile, int seconds) throws Exception {
        var ready = new java.util.concurrent.CountDownLatch(workers);
        var start = new java.util.concurrent.CountDownLatch(1);
        var deadline = new java.util.concurrent.atomic.AtomicLong();
        var futures = new ArrayList<java.util.concurrent.Future<Samples>>();
        long began;
        var all = new ArrayList<Long>();
        int errors = 0, within = 0;
        boolean capped = false;
        var failures = new TreeMap<String, Integer>();
        try (var executor = Executors.newFixedThreadPool(workers)) {
            try {
                for (int worker = 0; worker < workers; worker++) {
                    int id = worker;
                    futures.add(executor.submit(() -> {
                        var durations = new ArrayList<Long>();
                        var failed = new TreeMap<String, Integer>();
                        int inWindow = 0, sequence = 0, errorCount = 0;
                        ready.countDown();
                        start.await();
                        int limit = Math.max(1, 100_000 / workers);
                        while (System.nanoTime() < deadline.get() && !Thread.currentThread().isInterrupted() && sequence < limit) {
                            String record = recordsPrefix + "-" + id + "-" + sequence++;
                            long at = System.nanoTime();
                            try {
                                execute(db, table, profile, record, seedPrefix + "-origin-" + id, seedPrefix + "-step-" + id);
                                long finished = System.nanoTime();
                                durations.add(finished - at);
                                if (finished <= deadline.get()) inWindow++;
                            } catch (RuntimeException error) {
                                errorCount++;
                                failed.merge(error.getClass().getSimpleName(), 1, Integer::sum);
                            }
                        }
                        return new Samples(durations, errorCount, inWindow, failed, sequence >= limit && System.nanoTime() < deadline.get());
                    }));
                }
                if (!ready.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Benchmark workers not ready");
                began = System.nanoTime();
                deadline.set(began + TimeUnit.SECONDS.toNanos(seconds));
                start.countDown();
                for (var future : futures) {
                    var sample = future.get();
                    all.addAll(sample.durations()); errors += sample.errors(); within += sample.withinWindow();
                    capped |= sample.capped();
                    sample.failures().forEach((key, value) -> failures.merge(key, value, Integer::sum));
                }
            } finally {
                // Release waiting tasks BEFORE executor.close() drains them after startup failure.
                start.countDown();
            }
        }
        double elapsed = (System.nanoTime() - began) / 1e9;
        var result = new LinkedHashMap<String, Object>();
        result.put("profile", profile);
        result.put("logicalCallsPerSuccessfulRecord", profile.logicalCalls);
        result.put("inputWindowSeconds", seconds);
        result.put("elapsedIncludingDrainSeconds", elapsed);
        result.put("successfulRecords", all.size()); result.put("errors", errors); result.put("failures", failures);
        result.put("recordLimitReached", capped);
        result.put("completedWithinWindowRps", (double) within / seconds);
        result.put("successfulIncludingDrainRps", all.size() / elapsed);
        result.putAll(latencies(all));
        return result;
    }

    static Map<String, Object> latencies(List<Long> values) {
        var sorted = values.stream().sorted().toList();
        var result = new LinkedHashMap<String, Object>();
        result.put("meanSuccessfulRecordMs", sorted.stream().mapToLong(Long::longValue).average().orElse(0) / 1e6);
        result.put("p95SuccessfulRecordMs", sorted.isEmpty() ? null : sorted.get((int) Math.ceil(sorted.size() * .95) - 1) / 1e6);
        return result;
    }

    private record Timing(long count, double totalMs) { }
    private static Map<String, Timing> snapshot(SimpleMeterRegistry registry) {
        var result = new HashMap<String, Timing>();
        registry.getMeters().stream().filter(meter -> meter instanceof Timer).map(meter -> (Timer) meter)
                .forEach(timer -> result.put(timer.getId().getName() + timer.getId().getTags(),
                        new Timing(timer.count(), timer.totalTime(TimeUnit.MILLISECONDS))));
        return result;
    }
    private static List<Map<String, Object>> timers(SimpleMeterRegistry registry, Map<String, Timing> before) {
        var result = new ArrayList<Map<String, Object>>();
        registry.getMeters().stream().filter(meter -> meter instanceof Timer).map(meter -> (Timer) meter).forEach(timer -> {
            var initial = before.getOrDefault(timer.getId().getName() + timer.getId().getTags(), new Timing(0, 0));
            long count = timer.count() - initial.count();
            if (count > 0) result.add(Map.of("name", timer.getId().getName(), "tags", timer.getId().getTags().stream()
                            .collect(java.util.stream.Collectors.toMap(io.micrometer.core.instrument.Tag::getKey, io.micrometer.core.instrument.Tag::getValue)),
                    "count", count, "meanMs", (timer.totalTime(TimeUnit.MILLISECONDS) - initial.totalMs()) / count));
        });
        return result;
    }

    private static Map<String, AttributeValue> item(String id) {
        return Map.of("pk", AttributeValue.fromS(id), "payload", AttributeValue.fromS("x".repeat(1024)));
    }
    private static Map<String, AttributeValue> key(String id) { return Map.of("pk", AttributeValue.fromS(id)); }
    private static void save(Path output, JsonMapper mapper, Map<String, Object> report) throws Exception {
        Files.createDirectories(output.toAbsolutePath().getParent());
        Files.writeString(output, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report) + "\n");
    }
}

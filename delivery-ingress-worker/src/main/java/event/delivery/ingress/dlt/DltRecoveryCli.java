package event.delivery.ingress.dlt;

import event.common.delivery.DeliveryTopics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import tools.jackson.databind.json.JsonMapper;
import java.net.URI;
import java.sql.DriverManager;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import event.common.recovery.FailureBackoff;

/** Local PoC operations only. Credentials are read from environment, never from positional arguments or output. */
public final class DltRecoveryCli {
    public record IntakeCycle(DltIntakeWorker.Cycle intake, DltRecoveryResumer.Cycle resumed) { }
    public static void main(String[] args) {
        System.exit(run(args));
    }

    private static int run(String[] args) {
        boolean resume = args.length == 3 && Set.of("resume", "resume-once").contains(args[0]);
        boolean intake = args.length == 6 && Set.of("intake", "intake-once").contains(args[0]);
        if (!((args.length == 4 && args[0].equals("plan")) || (args.length == 7 && args[0].equals("apply")) || resume || intake)) {
            System.err.println("Usage: recover-dlt.sh plan <topic> <partition> <offset> OR apply <topic> <partition> <offset> <valueSha256> <actor> <reason> OR resume[-once] <limit:1..100> <intervalSeconds:1..3600> OR intake[-once] <topic> <partition> <initialOffset> <limit:1..100> <intervalSeconds:1..3600>");
            return 2;
        }
        try {
            String bootstrap = required("DLT_KAFKA_BOOTSTRAP_SERVERS");
            if (!bootstrap.matches("(localhost|127\\.0\\.0\\.1):[0-9]+")) throw new IllegalArgumentException("Local broker required");
            String cluster = required("DLT_CLUSTER_ALIAS");
            String endpoint = required("DLT_DYNAMODB_ENDPOINT");
            String sqlUrl = required("DLT_DB_URL");
            requireLocal(URI.create(endpoint)); requireLocal(URI.create(sqlUrl.replaceFirst("^jdbc:", "")));
            if (!endpoint.startsWith("http://") || !sqlUrl.startsWith("jdbc:postgresql://")) throw new IllegalArgumentException();
            var jdbc = new Properties();
            jdbc.setProperty("user", required("DLT_DB_USER")); jdbc.setProperty("password", required("DLT_DB_PASSWORD"));
            jdbc.setProperty("connectTimeout", "5"); jdbc.setProperty("socketTimeout", "15");
            jdbc.setProperty("options", "-c statement_timeout=5000");
            String schema = System.getenv().getOrDefault("DLT_DB_SCHEMA", "delivery_results");
            DltRecoveryStore.Connections connections = () -> DriverManager.getConnection(sqlUrl, jdbc);
            var store = new DltRecoveryStore(connections, schema);
            Duration ttl = Duration.parse(required("DLT_PRIMARY_TTL"));
            String sourceTopic = System.getenv().getOrDefault("DLT_SOURCE_TOPIC", DeliveryTopics.DELIVERY_REQUESTED);
            int limit = intake ? Integer.parseInt(args[4]) : resume ? Integer.parseInt(args[1]) : 0;
            int seconds = intake ? Integer.parseInt(args[5]) : resume ? Integer.parseInt(args[2]) : 0;
            if ((resume || intake) && (limit < 1 || limit > 100 || seconds < 1 || seconds > 3600)) throw new IllegalArgumentException("Invalid worker bounds");
            var record = resume || intake ? null : DltInspector.fetchExact(bootstrap, args[1], Integer.parseInt(args[2]), Long.parseLong(args[3]));
            var mapper = JsonMapper.builder().build();
            try (var db = DynamoDbClient.builder().endpointOverride(URI.create(endpoint)).region(Region.AP_NORTHEAST_2)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
                    .httpClientBuilder(Apache5HttpClient.builder().maxConnections(4).connectionTimeout(Duration.ofSeconds(2))
                            .connectionAcquisitionTimeout(Duration.ofSeconds(2)).socketTimeout(Duration.ofSeconds(5)))
                    .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(10)).apiCallAttemptTimeout(Duration.ofSeconds(5))).build()) {
                var planner = new DltRecoveryPlanner(db, store, ttl, sourceTopic, Clock.systemUTC());
                var plan = resume || intake ? null : planner.plan(record);
                if (args[0].equals("plan")) { System.out.println(mapper.writeValueAsString(plan.preview())); return 0; }
                if (!resume && !intake && (!plan.eligible() || !plan.preview().dlt().valueSha256().equals(args[4]))) {
                    System.out.println(mapper.writeValueAsString(plan.preview())); return 3;
                }
                var config = Map.<String, Object>of("bootstrap.servers", bootstrap, "acks", "all", "enable.idempotence", true,
                        "max.block.ms", 5000, "request.timeout.ms", 5000, "delivery.timeout.ms", 10000);
                try (var producer = new KafkaProducer<>(config, new StringSerializer(), new ByteArraySerializer())) {
                    DltRecoveryStore.Publisher publisher = command -> {
                        var ack = producer.send(new ProducerRecord<>(DeliveryTopics.DISPATCH_REQUESTED, command.deliveryId(),
                                mapper.writeValueAsBytes(command))).get(12, TimeUnit.SECONDS);
                        return new DltRecoveryStore.Ack(ack.topic(), ack.partition(), ack.offset());
                    };
                    if (intake) {
                        var backoff = workerBackoff();
                        var resumeBackoff = workerBackoff();
                        var scope = new DltIntakeStore.Scope(cluster, args[1], Integer.parseInt(args[2]), sourceTopic, DeliveryTopics.DISPATCH_REQUESTED);
                        long initialOffset = Long.parseLong(args[3]);
                        if (initialOffset < 0) throw new IllegalArgumentException("Initial offset required");
                        var worker = new DltIntakeWorker(new DltIntakeStore(connections, schema), store, planner, scope,
                                (offset, count) -> DltInspector.readRaw(bootstrap, scope.topic(), scope.partition(), offset, count), publisher, backoff);
                        var resumer = new DltRecoveryResumer(store, planner, cluster, DeliveryTopics.DISPATCH_REQUESTED, sourceTopic, publisher, resumeBackoff);
                        do {
                            try {
                                var cycle = worker.runOnce(initialOffset, limit);
                                var resumed = Set.of("BACKOFF", "BACKEND_UNAVAILABLE").contains(cycle.status())
                                        ? new DltRecoveryResumer.Cycle(0, List.of(), "INTAKE_DEFERRED") : resumer.runOnce(limit, Duration.ofSeconds(seconds));
                                System.out.println(mapper.writeValueAsString(new IntakeCycle(cycle, resumed)));
                                if (Set.of("RETENTION_GAP", "OFFSET_AFTER_END").contains(cycle.status())) return 3;
                                if (args[0].equals("intake-once")) return Set.of("SCANNED", "CURSOR_MOVED").contains(cycle.status())
                                        && cycle.backlog() != null && cycle.backlog().held() == 0 && cycle.backlog().unprocessed() == 0 && resumed.status().equals("SCANNED")
                                        && resumed.results().stream().allMatch(r -> Set.of("ACKNOWLEDGED", "ALREADY_ACKNOWLEDGED", "SKIPPED_CHANGED").contains(r.status())) ? 0 : 3;
                            } catch (RuntimeException failure) {
                                if (args[0].equals("intake-once")) throw failure;
                                System.err.println("DLT intake cycle unavailable: " + failure.getClass().getSimpleName());
                            }
                            if (Thread.currentThread().isInterrupted()) return 3;
                            pause(seconds, backoff, resumeBackoff);
                        } while (!Thread.currentThread().isInterrupted());
                        return 3;
                    }
                    if (resume) {
                        var backoff = workerBackoff();
                        var worker = new DltRecoveryResumer(store, planner, cluster, DeliveryTopics.DISPATCH_REQUESTED, sourceTopic, publisher, backoff);
                        do {
                            try {
                                var cycle = worker.runOnce(limit, Duration.ofSeconds(seconds));
                                System.out.println(mapper.writeValueAsString(cycle));
                                if (args[0].equals("resume-once")) return cycle.status().equals("SCANNED") && cycle.results().stream().allMatch(r ->
                                        Set.of("ACKNOWLEDGED", "ALREADY_ACKNOWLEDGED", "SKIPPED_CHANGED").contains(r.status())) ? 0 : 3;
                            } catch (RuntimeException failure) {
                                if (args[0].equals("resume-once")) throw failure;
                                System.err.println("DLT resume cycle unavailable: " + failure.getClass().getSimpleName());
                            }
                            if (Thread.currentThread().isInterrupted()) return 3;
                            pause(seconds, backoff);
                        } while (!Thread.currentThread().isInterrupted());
                        return 3;
                    }
                    var result = store.apply(cluster, DeliveryTopics.DISPATCH_REQUESTED, plan, args[5], args[6],
                            () -> planner.prepare(record, plan.command()), publisher, DltRecoveryCheckpoint.capture(plan, record));
                    System.out.println(mapper.writeValueAsString(result));
                    return Set.of("ACKNOWLEDGED", "ALREADY_ACKNOWLEDGED").contains(result.status()) ? 0 : 3;
                }
            }
        } catch (Exception failure) {
            System.err.println("DLT recovery unconfirmed; retain source and inspect operation records: " + failure.getClass().getSimpleName());
            return 1;
        }
    }
    private static FailureBackoff workerBackoff() {
        return new FailureBackoff(Duration.ofMillis(Long.parseLong(System.getenv().getOrDefault("DLT_FAILURE_INITIAL_DELAY_MS", "1000"))),
                Duration.ofMillis(Long.parseLong(System.getenv().getOrDefault("DLT_FAILURE_MAX_DELAY_MS", "30000"))));
    }
    private static void pause(int seconds, FailureBackoff... backoffs) throws InterruptedException {
        long millis = seconds * 1000L;
        for (var backoff : backoffs) millis = Math.max(millis, (long) Math.ceil(backoff.remainingSeconds() * 1000));
        Thread.sleep(millis);
    }
    private static String required(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing configuration");
        return value;
    }
    private static void requireLocal(URI uri) {
        if (!Set.of("localhost", "127.0.0.1").contains(uri.getHost()) || uri.getPort() < 1
                || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException("Local storage required");
        }
    }
}

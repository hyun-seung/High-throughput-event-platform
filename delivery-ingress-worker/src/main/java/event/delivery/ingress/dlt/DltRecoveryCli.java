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

/** Local PoC operations only. Credentials are read from environment, never from positional arguments or output. */
public final class DltRecoveryCli {
    public static void main(String[] args) {
        System.exit(run(args));
    }

    private static int run(String[] args) {
        if (!((args.length == 4 && args[0].equals("plan")) || (args.length == 7 && args[0].equals("apply")))) {
            System.err.println("Usage: recover-dlt.sh plan <topic> <partition> <offset> OR apply <topic> <partition> <offset> <valueSha256> <actor> <reason>");
            return 2;
        }
        try {
            String bootstrap = required("DLT_KAFKA_BOOTSTRAP_SERVERS");
            String cluster = required("DLT_CLUSTER_ALIAS");
            String endpoint = required("DLT_DYNAMODB_ENDPOINT");
            String sqlUrl = required("DLT_DB_URL");
            requireLocal(URI.create(endpoint)); requireLocal(URI.create(sqlUrl.replaceFirst("^jdbc:", "")));
            if (!endpoint.startsWith("http://") || !sqlUrl.startsWith("jdbc:postgresql://")) throw new IllegalArgumentException();
            var jdbc = new Properties();
            jdbc.setProperty("user", required("DLT_DB_USER")); jdbc.setProperty("password", required("DLT_DB_PASSWORD"));
            jdbc.setProperty("connectTimeout", "5"); jdbc.setProperty("socketTimeout", "15");
            jdbc.setProperty("options", "-c statement_timeout=5000");
            var store = new DltRecoveryStore(() -> DriverManager.getConnection(sqlUrl, jdbc),
                    System.getenv().getOrDefault("DLT_DB_SCHEMA", "delivery_results"));
            Duration ttl = Duration.parse(required("DLT_PRIMARY_TTL"));
            String sourceTopic = System.getenv().getOrDefault("DLT_SOURCE_TOPIC", DeliveryTopics.DELIVERY_REQUESTED);
            var record = DltInspector.fetchExact(bootstrap, args[1], Integer.parseInt(args[2]), Long.parseLong(args[3]));
            var mapper = JsonMapper.builder().build();
            try (var db = DynamoDbClient.builder().endpointOverride(URI.create(endpoint)).region(Region.AP_NORTHEAST_2)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
                    .httpClientBuilder(Apache5HttpClient.builder().maxConnections(4).connectionTimeout(Duration.ofSeconds(2))
                            .connectionAcquisitionTimeout(Duration.ofSeconds(2)).socketTimeout(Duration.ofSeconds(5)))
                    .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(10)).apiCallAttemptTimeout(Duration.ofSeconds(5))).build()) {
                var planner = new DltRecoveryPlanner(db, store, ttl, sourceTopic, Clock.systemUTC());
                var plan = planner.plan(record);
                if (args[0].equals("plan")) { System.out.println(mapper.writeValueAsString(plan.preview())); return 0; }
                if (!plan.eligible() || !plan.preview().dlt().valueSha256().equals(args[4])) {
                    System.out.println(mapper.writeValueAsString(plan.preview())); return 3;
                }
                var config = Map.<String, Object>of("bootstrap.servers", bootstrap, "acks", "all", "enable.idempotence", true,
                        "max.block.ms", 5000, "request.timeout.ms", 5000, "delivery.timeout.ms", 10000);
                try (var producer = new KafkaProducer<>(config, new StringSerializer(), new ByteArraySerializer())) {
                    var result = store.apply(cluster, DeliveryTopics.DISPATCH_REQUESTED, plan, args[5], args[6], () -> planner.plan(record), command -> {
                        var ack = producer.send(new ProducerRecord<>(DeliveryTopics.DISPATCH_REQUESTED, command.deliveryId(),
                                mapper.writeValueAsBytes(command))).get(12, TimeUnit.SECONDS);
                        return new DltRecoveryStore.Ack(ack.topic(), ack.partition(), ack.offset());
                    });
                    System.out.println(mapper.writeValueAsString(result));
                    return Set.of("ACKNOWLEDGED", "ALREADY_ACKNOWLEDGED").contains(result.status()) ? 0 : 3;
                }
            }
        } catch (Exception failure) {
            System.err.println("DLT recovery unconfirmed; retain source and inspect operation records: " + failure.getClass().getSimpleName());
            return 1;
        }
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

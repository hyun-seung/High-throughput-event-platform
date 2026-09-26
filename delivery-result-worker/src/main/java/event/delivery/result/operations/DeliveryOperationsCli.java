package event.delivery.result.operations;

import com.zaxxer.hikari.*;
import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import tools.jackson.databind.json.JsonMapper;
import java.net.URI;
import java.time.*;
import java.util.*;

/** Local read-only CLI, not a Spring application: schedulers and Kafka listeners are never started. */
public final class DeliveryOperationsCli {
    public static void main(String[] args) { System.exit(run(args)); }
    static int run(String[] args) {
        boolean request = args.length == 3 && args[0].equals("request");
        if (!(request || (args.length == 4 && Set.of("notifications", "cleanup").contains(args[0])))) {
            System.err.println("Usage: inspect-delivery.sh request <tenantId> <requestKey UUID> OR notifications|cleanup <tenantId> <afterId UUID or -> <limit:1..100>");
            return 2;
        }
        try {
            String url = required("OPS_DB_URL"), schema = System.getenv().getOrDefault("OPS_DB_SCHEMA", "delivery_results");
            if (!url.startsWith("jdbc:postgresql://") || !schema.matches("[a-z][a-z0-9_]{0,62}")) throw new IllegalArgumentException("Invalid SQL configuration");
            local(URI.create(url.substring(5)));
            var config = new HikariConfig(); config.setJdbcUrl(url); config.setUsername(required("OPS_DB_USER")); config.setPassword(required("OPS_DB_PASSWORD"));
            config.setSchema(schema); config.setReadOnly(true); config.setMaximumPoolSize(1); config.setConnectionTimeout(5000);
            config.addDataSourceProperty("connectTimeout", "5"); config.addDataSourceProperty("socketTimeout", "15");
            config.addDataSourceProperty("options", "-c statement_timeout=5000");
            try (var pool = new HikariDataSource(config); var db = request ? dynamo() : null) {
                var sql = new JdbcTemplate(pool); sql.setQueryTimeout(5);
                var operations = new DeliveryOperations(sql, db, Clock.systemUTC());
                long tenant = Long.parseLong(args[1]);
                Object result = request ? operations.request(tenant, args[2]) : args[0].equals("notifications")
                        ? operations.exhausted(tenant, args[2].equals("-") ? null : UUID.fromString(args[2]), Integer.parseInt(args[3]))
                        : operations.cleanup(tenant, args[2].equals("-") ? null : UUID.fromString(args[2]), Integer.parseInt(args[3]));
                System.out.println(JsonMapper.builder().build().writeValueAsString(result)); return 0;
            }
        } catch (Exception failure) {
            System.err.println("Delivery inspection unavailable (no writes): " + failure.getClass().getSimpleName()); return 1;
        }
    }
    static DynamoDbClient dynamo() {
        String endpoint = required("OPS_DYNAMODB_ENDPOINT"); var uri = URI.create(endpoint); local(uri);
        if (!endpoint.startsWith("http://")) throw new IllegalArgumentException("Local DynamoDB required");
        return DynamoDbClient.builder().endpointOverride(uri).region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(10)).apiCallAttemptTimeout(Duration.ofSeconds(5))).build();
    }
    private static void local(URI uri) {
        if (!Set.of("localhost", "127.0.0.1").contains(uri.getHost()) || uri.getPort() < 1 || uri.getUserInfo() != null
                || uri.getQuery() != null || uri.getFragment() != null) throw new IllegalArgumentException("Local endpoint required");
    }
    private static String required(String key) {
        String value = System.getenv(key); if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing configuration"); return value;
    }
}

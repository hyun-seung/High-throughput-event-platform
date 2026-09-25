package event.delivery.ingress.dlt;

import tools.jackson.databind.json.JsonMapper;
import java.net.URI;
import java.sql.DriverManager;
import java.time.Instant;
import java.util.*;

/** SQL-only local PoC operations. No Kafka producer, DynamoDB client or provider connection. */
public final class DltOperationsCli {
    public static void main(String[] args) { System.exit(run(args)); }
    static int run(String[] args) {
        if (!((args.length == 5 && args[0].equals("held")) || (args.length == 2 && args[0].equals("status"))
                || (args.length == 6 && args[0].equals("recheck")))) {
            System.err.println("Usage: operate-dlt.sh held <topic> <partition> <afterOffset:-1 for first page> <limit:1..100> OR status <intakeId> OR recheck <intakeId> <updatedAt> <actionId> <actor> <reason>");
            return 2;
        }
        try {
            String url = required("DLT_DB_URL");
            URI uri = URI.create(url.replaceFirst("^jdbc:", ""));
            if (!url.startsWith("jdbc:postgresql://") || !Set.of("localhost", "127.0.0.1").contains(uri.getHost())
                    || uri.getPort() < 1 || uri.getQuery() != null || uri.getFragment() != null || uri.getUserInfo() != null)
                throw new IllegalArgumentException("Local SQL required");
            var jdbc = new Properties();
            jdbc.setProperty("user", required("DLT_DB_USER")); jdbc.setProperty("password", required("DLT_DB_PASSWORD"));
            jdbc.setProperty("connectTimeout", "5"); jdbc.setProperty("socketTimeout", "15");
            jdbc.setProperty("options", "-c statement_timeout=5000");
            var operations = new DltOperations(() -> DriverManager.getConnection(url, jdbc),
                    System.getenv().getOrDefault("DLT_DB_SCHEMA", "delivery_results"), required("DLT_CLUSTER_ALIAS"));
            Object result = switch (args[0]) {
                case "held" -> operations.held(args[1], Integer.parseInt(args[2]), Long.parseLong(args[3]), Integer.parseInt(args[4]));
                case "status" -> operations.status(UUID.fromString(args[1]));
                default -> operations.recheck(UUID.fromString(args[1]), Instant.parse(args[2]), UUID.fromString(args[3]), args[4], args[5]);
            };
            System.out.println(JsonMapper.builder().build().writeValueAsString(result));
            if (result == null) return 3;
            if (result instanceof DltOperations.Recheck recheck && !Set.of("QUEUED", "ALREADY_QUEUED").contains(recheck.status())) return 3;
            return 0;
        } catch (Exception failure) {
            System.err.println("DLT operation unconfirmed: " + failure.getClass().getSimpleName());
            return 1;
        }
    }
    private static String required(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing configuration");
        return value;
    }
}

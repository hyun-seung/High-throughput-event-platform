package event.delivery.result.operations;

import com.zaxxer.hikari.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import tools.jackson.databind.json.JsonMapper;
import java.net.URI;
import java.util.*;

/** Local operator action. Does not start Spring workers, call providers or access DynamoDB. */
public final class CleanupOperationsCli {
    public static void main(String[] args) { System.exit(run(args)); }
    static int run(String[] args) {
        if (args.length != 7 || !args[0].equals("retry")) {
            System.err.println("Usage: retry-cleanup.sh retry <tenantId> <resultEventId UUID> <expectedVersion> <actionId UUID> <actor> <reason>");
            return 2;
        }
        try {
            var request = new CleanupOperations.Request(Long.parseLong(args[1]), UUID.fromString(args[2]), Long.parseLong(args[3]),
                    UUID.fromString(args[4]), args[5], args[6]);
            String url = required("OPS_DB_URL"), schema = System.getenv().getOrDefault("OPS_DB_SCHEMA", "delivery_results");
            if (!url.startsWith("jdbc:postgresql://") || !schema.matches("[a-z][a-z0-9_]{0,62}")) throw new IllegalArgumentException("Invalid SQL configuration");
            var uri = URI.create(url.substring(5));
            if (!Set.of("localhost", "127.0.0.1").contains(uri.getHost()) || uri.getPort() < 1 || uri.getUserInfo() != null
                    || uri.getQuery() != null || uri.getFragment() != null) throw new IllegalArgumentException("Local endpoint required");
            var config = new HikariConfig(); config.setJdbcUrl(url); config.setUsername(required("OPS_DB_USER")); config.setPassword(required("OPS_DB_PASSWORD"));
            config.setSchema(schema); config.setMaximumPoolSize(1); config.setConnectionTimeout(5000);
            config.addDataSourceProperty("connectTimeout", "5"); config.addDataSourceProperty("socketTimeout", "15");
            config.addDataSourceProperty("options", "-c statement_timeout=5000 -c lock_timeout=2000");
            try (var pool = new HikariDataSource(config)) {
                var sql = new JdbcTemplate(pool); sql.setQueryTimeout(5);
                var outcome = new CleanupOperations(sql, new DataSourceTransactionManager(pool)).retry(request);
                System.out.println(JsonMapper.builder().build().writeValueAsString(Map.of("actionId", request.actionId(), "outcome", outcome)));
                return outcome == CleanupOperations.Outcome.QUEUED || outcome == CleanupOperations.Outcome.ALREADY_QUEUED ? 0 : 3;
            }
        } catch (Exception failure) {
            System.err.println("Cleanup action outcome unconfirmed; retry with identical action ID and arguments: " + failure.getClass().getSimpleName()); return 1;
        }
    }
    private static String required(String key) {
        String value = System.getenv(key); if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing configuration"); return value;
    }
}

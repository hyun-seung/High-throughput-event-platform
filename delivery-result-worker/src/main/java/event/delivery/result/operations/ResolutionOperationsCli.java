package event.delivery.result.operations;

import com.zaxxer.hikari.*;
import event.common.lifecycle.ManualResolution;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import tools.jackson.databind.json.JsonMapper;
import java.net.URI;
import java.time.Clock;
import java.util.*;

/** Explicit local operator action, without application listeners or schedulers. */
public final class ResolutionOperationsCli {
    public static void main(String[] args) { System.exit(run(args)); }
    static int run(String[] args) {
        boolean decide = args.length == 10 && args[0].equals("decide"), resume = args.length == 3 && args[0].equals("resume");
        boolean pending = args.length == 4 && args[0].equals("pending");
        if (!(decide || resume || pending)) {
            System.err.println("Usage: resolve-delivery.sh decide <tenantId> <requestKey UUID> <deliveryId UUID> <attemptId UUID> <version> <actionId UUID> <SUCCEEDED|FAILED> <actor> <reason> OR resume <tenantId> <actionId UUID> OR pending <tenantId> <afterId UUID or -> <limit:1..100>");
            return 2;
        }
        try {
            long tenant = Long.parseLong(args[1]);
            var request = decide ? new ResolutionOperations.Request(new ManualResolution.Target(tenant, UUID.fromString(args[2]), UUID.fromString(args[3]),
                    UUID.fromString(args[4]), Long.parseLong(args[5]), UUID.fromString(args[6]), ManualResolution.Decision.valueOf(args[7])), args[8], args[9]) : null;
            String url = required("OPS_DB_URL"), schema = System.getenv().getOrDefault("OPS_DB_SCHEMA", "delivery_results");
            if (!url.startsWith("jdbc:postgresql://") || !schema.matches("[a-z][a-z0-9_]{0,62}")) throw new IllegalArgumentException("Invalid SQL configuration");
            var uri = URI.create(url.substring(5));
            if (!Set.of("localhost", "127.0.0.1").contains(uri.getHost()) || uri.getPort() < 1 || uri.getUserInfo() != null
                    || uri.getQuery() != null || uri.getFragment() != null) throw new IllegalArgumentException("Local endpoint required");
            var config = new HikariConfig(); config.setJdbcUrl(url); config.setUsername(required("OPS_DB_USER")); config.setPassword(required("OPS_DB_PASSWORD"));
            config.setSchema(schema); config.setMaximumPoolSize(1); config.setReadOnly(pending); config.setConnectionTimeout(5000);
            config.addDataSourceProperty("connectTimeout", "5"); config.addDataSourceProperty("socketTimeout", "25");
            config.addDataSourceProperty("options", "-c statement_timeout=20000 -c lock_timeout=2000");
            try (var pool = new HikariDataSource(config); var db = pending ? null : DeliveryOperationsCli.dynamo()) {
                var sql = new JdbcTemplate(pool); sql.setQueryTimeout(20);
                var operations = new ResolutionOperations(sql, new DataSourceTransactionManager(pool), db == null ? null : new ManualResolution(db), Clock.systemUTC());
                Object outcome = pending ? operations.pending(tenant, args[2].equals("-") ? null : UUID.fromString(args[2]), Integer.parseInt(args[3]))
                        : decide ? operations.resolve(request) : operations.resume(tenant, UUID.fromString(args[2]));
                System.out.println(JsonMapper.builder().build().writeValueAsString(Map.of("result", outcome)));
                return outcome == ResolutionOperations.Outcome.REJECTED || outcome == ResolutionOperations.Outcome.ACTION_CONFLICT
                        || outcome == ResolutionOperations.Outcome.PENDING_RECONCILIATION ? 3 : 0;
            }
        } catch (Exception failure) {
            System.err.println("Resolution outcome unconfirmed; inspect pending actions and resume the same action ID: " + failure.getClass().getSimpleName()); return 1;
        }
    }
    private static String required(String key) {
        String value = System.getenv(key); if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing configuration"); return value;
    }
}

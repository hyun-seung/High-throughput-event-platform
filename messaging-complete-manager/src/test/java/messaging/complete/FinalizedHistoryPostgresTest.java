package messaging.complete;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import messaging.common.messages.FinalizedMessageResult;
import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.PrimaryStageDecision;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "POSTGRES_TEST_URL", matches = ".+")
class FinalizedHistoryPostgresTest {
    private final String schema = "completion_test_" + UUID.randomUUID().toString().replace("-", "");
    private final JsonMapper mapper = JsonMapper.builder().build();
    private HikariDataSource pool;
    private JdbcTemplate jdbc;
    private FinalizedHistoryStore history;

    @BeforeEach
    void connect() {
        String url = System.getenv("POSTGRES_TEST_URL");
        URI endpoint = URI.create(url.substring("jdbc:".length()));
        if (!Set.of("localhost", "127.0.0.1").contains(endpoint.getHost())) {
            throw new IllegalArgumentException("Local test database required");
        }
        String user = System.getenv().getOrDefault("POSTGRES_TEST_USER", "delivery");
        String password = System.getenv().getOrDefault("POSTGRES_TEST_PASSWORD", "delivery");
        Flyway.configure().dataSource(url, user, password).defaultSchema(schema).schemas(schema).load().migrate();
        var config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setSchema(schema);
        pool = new HikariDataSource(config);
        jdbc = new JdbcTemplate(pool);
        history = new FinalizedHistoryStore(jdbc, new DataSourceTransactionManager(pool), mapper);
    }

    @AfterEach
    void cleanup() {
        if (pool == null) return;
        try { jdbc.execute("DROP SCHEMA " + schema + " CASCADE"); }
        finally { pool.close(); }
    }

    @Test
    void successCreatesOneMessageAndOneCdrWhileFailureHasNoCdr() {
        var success = result("a".repeat(32), PrimaryStageDecision.Kind.SUCCESS);
        var failure = result("b".repeat(32), PrimaryStageDecision.Kind.FAILURE);

        history.store(success);
        history.store(success);
        history.store(failure);
        history.store(failure);

        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
        assertEquals(success.decision().decisionId(), jdbc.queryForObject(
                "SELECT decision_id FROM tbl_cdr_hist WHERE client_msg_id = ?", String.class,
                success.decision().clientMsgId()));
    }

    @Test
    void conflictingFinalResultCannotChangeCommittedHistoryOrBilling() {
        var original = result("a".repeat(32), PrimaryStageDecision.Kind.SUCCESS);
        history.store(original);
        var conflicting = new FinalizedMessageResult(new PrimaryStageDecision(
                "other-decision", original.decision().clientMsgId(), PrimaryStageDecision.Kind.FAILURE,
                "WEBHOOK", 66001, null, HttpCarrier.SKT, 1, false,
                original.decision().decidedAt()), original.submission());

        assertThrows(IllegalStateException.class, () -> history.store(conflicting));

        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
        assertEquals(original.decision().decisionId(), jdbc.queryForObject(
                "SELECT decision_id FROM tbl_msg_hist WHERE client_msg_id = ?", String.class,
                original.decision().clientMsgId()));
    }

    @Test
    void concurrentRedeliveryStillCreatesOneMessageAndOneCdr() throws Exception {
        var finalized = result("c".repeat(32), PrimaryStageDecision.Kind.SUCCESS);
        try (var threads = Executors.newFixedThreadPool(8)) {
            var deliveries = new ArrayList<Future<?>>();
            for (int i = 0; i < 24; i++) deliveries.add(threads.submit(() -> history.store(finalized)));
            for (var delivery : deliveries) delivery.get(20, TimeUnit.SECONDS);
        }

        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_msg_hist", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_cdr_hist", Integer.class));
    }

    private static FinalizedMessageResult result(String id, PrimaryStageDecision.Kind kind) {
        var now = Instant.parse("2026-10-07T12:00:00Z");
        var decision = new PrimaryStageDecision("decision-" + id, id, kind, "WEBHOOK",
                kind == PrimaryStageDecision.Kind.FAILURE ? 66999 : null, null,
                HttpCarrier.SKT, 1, false, now);
        var submission = new MessageSubmission(id, 42L, "customer-message", "01012345678",
                MessageCategory.GENERAL, Map.of("text", "hello"), null, now.minusSeconds(30));
        return new FinalizedMessageResult(decision, submission);
    }
}

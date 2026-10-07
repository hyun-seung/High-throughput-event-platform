package messaging.webhook.sender;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import messaging.common.messages.CustomerWebhookSendCommand;
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
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "POSTGRES_TEST_URL", matches = ".+")
class WebhookBatchRepositoryPostgresTest {
    private final String schema = "webhook_test_" + UUID.randomUUID().toString().replace("-", "");
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final WebhookSenderProperties.Destination destination = new WebhookSenderProperties.Destination(
            URI.create("https://example.org/results"), "a".repeat(32));
    private final WebhookSenderProperties settings = new WebhookSenderProperties(true, 100, 4,
            Duration.ZERO, 262144, Duration.ofSeconds(3), Duration.ofSeconds(30),
            Duration.ofMillis(1), Duration.ofSeconds(60), Map.of(42L, destination, 43L, destination));
    private HikariDataSource pool;
    private JdbcTemplate jdbc;
    private WebhookBatchRepository repository;

    @BeforeEach
    void connect() {
        String url = System.getenv("POSTGRES_TEST_URL");
        URI endpoint = URI.create(url.substring("jdbc:".length()));
        if (!Set.of("localhost", "127.0.0.1").contains(endpoint.getHost())) throw new IllegalArgumentException("Local test database required");
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
        repository = new WebhookBatchRepository(jdbc, new DataSourceTransactionManager(pool), mapper);
    }

    @AfterEach
    void cleanup() {
        if (pool == null) return;
        try { jdbc.execute("DROP SCHEMA " + schema + " CASCADE"); }
        finally { pool.close(); }
    }

    @Test
    void splits101ResultsForOneCustomerAndKeepsAnotherCustomerSeparate() {
        for (int i = 0; i < 101; i++) repository.capture(command(i, 42));
        repository.capture(command(999, 43));
        repository.capture(command(0, 42));

        var first = repository.claim(42, destination, settings).orElseThrow();
        assertEquals(100, first.size());
        var firstBody = mapper.readValue(first.body(), CustomerWebhookBatch.class);
        assertEquals(42, firstBody.clientId());
        assertEquals(100, firstBody.results().size());
        assertEquals(100, firstBody.results().stream().map(CustomerWebhookBatch.Result::clientMsgId).distinct().count());
        assertTrue(repository.complete(first, new CustomerWebhookClient.Result(true, 204, null), Duration.ZERO));

        var second = repository.claim(42, destination, settings).orElseThrow();
        assertEquals(1, second.size());
        assertNotEquals(first.batchId(), second.batchId());
        var other = repository.claim(43, destination, settings).orElseThrow();
        assertEquals(1, other.size());
        assertEquals(43, mapper.readValue(other.body(), CustomerWebhookBatch.class).clientId());
        assertEquals(102, jdbc.queryForObject("SELECT count(*) FROM tbl_webhook_outbox", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_webhook_hist", Integer.class));
    }

    @Test
    void retryKeepsBatchIdBodyAndMembershipAndRecordsEachAttempt() {
        repository.capture(command(1, 42));
        var first = repository.claim(42, destination, settings).orElseThrow();
        assertTrue(repository.complete(first, new CustomerWebhookClient.Result(false, 503, "HTTP_503"), Duration.ZERO));
        var second = repository.claim(42, destination, settings).orElseThrow();
        assertEquals(first.batchId(), second.batchId());
        assertEquals(first.body(), second.body());
        assertEquals(2, second.attempt());
        assertTrue(repository.complete(second, new CustomerWebhookClient.Result(true, 204, null), Duration.ZERO));
        assertEquals("DELIVERED", jdbc.queryForObject("SELECT status FROM tbl_webhook_outbox", String.class));
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM tbl_webhook_hist", Integer.class));
    }

    @Test
    void duplicateCommandIsIdempotentButDifferentFinalDecisionConflicts() {
        var original = command(1, 42);
        repository.capture(original);
        repository.capture(original);
        var changed = new CustomerWebhookSendCommand(original.webhookId(), new FinalizedMessageResult(
                new PrimaryStageDecision("changed", original.finalized().decision().clientMsgId(),
                        PrimaryStageDecision.Kind.FAILURE, "WEBHOOK", 66003, null,
                        HttpCarrier.SKT, 1, false, Instant.parse("2026-10-07T12:00:00Z")),
                original.finalized().submission()));
        assertThrows(IllegalStateException.class, () -> repository.capture(changed));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tbl_webhook_outbox", Integer.class));
    }

    @Test
    void unconfirmedLastAttemptIsExhaustedWithAuditRow() {
        repository.capture(command(1, 42));
        var first = repository.claim(42, destination, settings).orElseThrow();
        jdbc.update("UPDATE tbl_webhook_batch SET attempt_count = 21, lease_until = clock_timestamp() - interval '1 second' WHERE batch_id = ?", first.batchId());

        assertTrue(repository.claim(42, destination, settings).isEmpty());
        assertEquals("EXHAUSTED", jdbc.queryForObject("SELECT status FROM tbl_webhook_outbox", String.class));
        assertEquals("LAST_ATTEMPT_UNCONFIRMED", jdbc.queryForObject("SELECT error FROM tbl_webhook_hist", String.class));
    }

    private static CustomerWebhookSendCommand command(int index, long clientId) {
        String id = "%032d".formatted(index);
        var now = Instant.parse("2026-10-07T12:00:00Z");
        var decision = new PrimaryStageDecision("decision-" + id, id, PrimaryStageDecision.Kind.SUCCESS,
                "WEBHOOK", null, null, HttpCarrier.SKT, 1, false, now);
        var submission = new MessageSubmission(id, clientId, "customer-" + index, "01012345678",
                MessageCategory.GENERAL, Map.of("text", "hello"), null, now.minusSeconds(30));
        return CustomerWebhookSendCommand.from(new FinalizedMessageResult(decision, submission));
    }
}

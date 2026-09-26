package event.common.dynamodb;

import com.sun.net.httpserver.HttpServer;
import event.common.dynamodb.config.DynamoDbAutoConfiguration;
import event.common.dynamodb.config.DynamoDbProperties;
import event.common.recovery.StorageFailure;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import static org.junit.jupiter.api.Assertions.*;

/** Real AWS SDK and HTTP sockets; the local server models DynamoDB transport responses, not its storage semantics. */
class DynamoDbClientRecoveryTest {
    record Reply(int code, String body, boolean trickle) { }
    static final Reply OK = new Reply(200, "{}", false);
    static final Reply UNAVAILABLE = new Reply(503, "{\"__type\":\"InternalServerError\",\"message\":\"unavailable\"}", false);
    final AtomicInteger calls = new AtomicInteger();
    final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    final java.util.concurrent.ExecutorService handlers = Executors.newFixedThreadPool(4);
    volatile IntFunction<Reply> replies = ignored -> OK;
    HttpServer server;

    @BeforeEach void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(handlers);
        server.createContext("/", exchange -> {
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                Reply reply = replies.apply(calls.incrementAndGet());
                byte[] body = (reply.trickle() ? " ".repeat(256) + reply.body() : reply.body()).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.0");
                exchange.sendResponseHeaders(reply.code(), body.length);
                if (reply.trickle()) {
                    for (byte value : body) {
                        exchange.getResponseBody().write(value); exchange.getResponseBody().flush();
                        Thread.sleep(40); // Bytes arrive before socket timeout; only an API budget stops this response.
                    }
                } else exchange.getResponseBody().write(body);
            } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            catch (java.io.IOException disconnected) { /* Expected when the SDK cancels a timed-out response. */ }
        });
        server.start();
    }
    @AfterEach void close() {
        if (server != null) server.stop(0);
        handlers.shutdownNow(); meters.close();
    }
    String endpoint() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
    DynamoDbProperties properties() {
        var properties = new DynamoDbProperties(); properties.setEndpoint(endpoint()); return properties;
    }
    DynamoDbClient client(DynamoDbProperties properties) {
        var beans = new StaticListableBeanFactory(); beans.addBean("meters", meters);
        return new DynamoDbAutoConfiguration().dynamoDbClient(properties, beans.getBeanProvider(MeterRegistry.class));
    }
    static void read(DynamoDbClient client) {
        client.getItem(r -> r.tableName("ORIGIN").key(Map.of("pk", AttributeValue.fromS("request"), "sk", AttributeValue.fromS("META"))));
    }

    @Test void transientFailuresRetryAndMetricsSeparateLogicalCallFromTransportAttempts() {
        replies = attempt -> attempt < 3 ? UNAVAILABLE : OK;
        try (var client = client(properties())) { read(client); }
        assertEquals(3, calls.get());
        assertEquals(3, meters.get("delivery.dynamodb.transport.attempts").tag("operation", "get_item").counter().count());
        assertEquals(1, meters.get("delivery.dynamodb.duration").tags("operation", "get_item", "result", "success").timer().count());
    }

    @Test void persistentFailureStopsAtConfiguredBudgetAndSameClientRecovers() {
        replies = ignored -> UNAVAILABLE;
        try (var client = client(properties())) {
            assertThrows(DynamoDbException.class, () -> read(client)); assertEquals(3, calls.get());
            replies = ignored -> OK; read(client); assertEquals(4, calls.get());
        }
        assertEquals(1, meters.get("delivery.dynamodb.duration").tag("result", "failure").timer().count());
        assertEquals(1, meters.get("delivery.dynamodb.duration").tag("result", "success").timer().count());
    }

    @Test void failedConditionalWriteIsNotRetriedOrClassifiedAsAnOutage() {
        replies = ignored -> new Reply(400, "{\"__type\":\"ConditionalCheckFailedException\",\"message\":\"occupied\"}", false);
        try (var client = client(properties())) {
            var failure = assertThrows(ConditionalCheckFailedException.class,
                    () -> client.putItem(r -> r.tableName("STEP").item(Map.of("pk", AttributeValue.fromS("attempt")))
                            .conditionExpression("attribute_not_exists(pk)")));
            assertFalse(StorageFailure.unavailable(failure)); assertEquals(1, calls.get());
        }
        assertEquals(1, meters.get("delivery.dynamodb.duration").tag("result", "condition_failed").timer().count());
    }

    DynamoDbProperties shortBudgets() {
        var properties = properties();
        properties.setConnectTimeout(Duration.ofMillis(400)); properties.setAcquireTimeout(Duration.ofMillis(400));
        properties.setSocketTimeout(Duration.ofMillis(400)); properties.setApiCallAttemptTimeout(Duration.ofMillis(600));
        properties.setApiCallTimeout(Duration.ofMillis(2000)); return properties;
    }

    @Test void attemptTimeoutStopsAResponseThatContinuesSendingBytesAndClientRemainsUsable() {
        var properties = shortBudgets(); properties.setMaxAttempts(1);
        try (var client = client(properties)) {
            read(client); replies = ignored -> new Reply(200, "{}", true);
            var failure = assertThrows(ApiCallAttemptTimeoutException.class, () -> read(client));
            assertTrue(StorageFailure.unavailable(failure)); assertEquals(2, calls.get());
            replies = ignored -> OK; read(client); assertEquals(3, calls.get());
        }
    }

    @Test void totalTimeoutBoundsMultipleSlowAttempts() {
        var properties = shortBudgets(); properties.setApiCallTimeout(Duration.ofMillis(900)); properties.setMaxAttempts(10);
        try (var client = client(properties)) {
            read(client); replies = ignored -> new Reply(200, "{}", true);
            long start = System.nanoTime();
            assertThrows(ApiCallTimeoutException.class, () -> read(client));
            assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(4)) < 0);
            assertTrue(calls.get() >= 2 && calls.get() <= 3); // Includes the warmup; never exhausts ten attempts.
        }
    }

    @Test void springBindingAppliesBudgetsWithoutAMetricsRegistry() {
        replies = ignored -> UNAVAILABLE;
        try (var context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("test", Map.of(
                    "delivery.dynamodb.enabled", "true", "delivery.dynamodb.endpoint", endpoint(),
                    "delivery.dynamodb.api-call-timeout", "8s", "delivery.dynamodb.api-call-attempt-timeout", "6s",
                    "delivery.dynamodb.max-attempts", "1")));
            context.register(DynamoDbAutoConfiguration.class); context.refresh();
            var client = context.getBean(DynamoDbClient.class);
            assertEquals(Duration.ofSeconds(8), client.serviceClientConfiguration().overrideConfiguration().apiCallTimeout().orElseThrow());
            assertEquals(Duration.ofSeconds(6), client.serviceClientConfiguration().overrideConfiguration().apiCallAttemptTimeout().orElseThrow());
            assertThrows(DynamoDbException.class, () -> read(client)); assertEquals(1, calls.get());
        }
    }

    @Test void invalidBudgetHierarchyAndAttemptCountFailBeforeClientCreation() {
        var properties = properties(); properties.setApiCallTimeout(Duration.ofSeconds(4));
        assertThrows(IllegalArgumentException.class, () -> client(properties));
        properties.setApiCallTimeout(Duration.ofSeconds(10)); properties.setApiCallAttemptTimeout(Duration.ofSeconds(1));
        assertThrows(IllegalArgumentException.class, () -> client(properties));
        properties.setApiCallAttemptTimeout(Duration.ofSeconds(5)); properties.setMaxAttempts(0);
        assertThrows(IllegalArgumentException.class, () -> client(properties));
        properties.setMaxAttempts(11); assertThrows(IllegalArgumentException.class, () -> client(properties));
        properties.setMaxAttempts(3); properties.setApiCallTimeout(Duration.ZERO);
        assertThrows(IllegalArgumentException.class, () -> client(properties));
    }
}

package event.receipt;

import event.common.receipt.ReceiptEvent;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Full production HTTP/security/serializer/producer path; touches only this UUID topic. */
@EnabledIfEnvironmentVariable(named = "KAFKA_TEST_BOOTSTRAP_SERVERS", matches = ".+")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "management.server.port=0", "spring.mvc.async.request-timeout=5s",
        "receipt.primary-secret=" + ReceiptHttpTest.PRIMARY,
        "spring.kafka.producer.properties[max.block.ms]=500",
        "spring.kafka.producer.properties[request.timeout.ms]=500",
        "spring.kafka.producer.properties[delivery.timeout.ms]=1500"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ReceiptKafkaIntegrationTest {
    static final String TOPIC = "test.receipt-http." + UUID.randomUUID();
    @Autowired Environment environment;
    @Autowired JsonMapper mapper;

    static String bootstrap() {
        String bootstrap = System.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS");
        for (String address : bootstrap.split(",")) {
            if (!address.trim().matches("(localhost|127\\.0\\.0\\.1):[0-9]+")) {
                throw new IllegalArgumentException("Receipt integration tests require a local broker");
            }
        }
        return bootstrap;
    }

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", ReceiptKafkaIntegrationTest::bootstrap);
        registry.add("receipt.topic", () -> TOPIC);
    }

    @AfterAll
    static void cleanUp() throws Exception {
        try (var admin = admin()) { admin.deleteTopics(List.of(TOPIC)).all().get(10, TimeUnit.SECONDS); }
    }

    @Test
    void acknowledgedReceiptsSurviveHttpRetryAsTwoRecordsWithStableLogicalIdentity() throws Exception {
        String body = ReceiptHttpTest.body();
        var first = post(body);
        var repeat = post(body);
        assertEquals(202, first.statusCode());
        assertEquals(202, repeat.statusCode());
        assertEquals(mapper.readTree(first.body()).get("eventId"), mapper.readTree(repeat.body()).get("eventId"));
        var records = read(mapper.readTree(body).get("deliveryId").asText(), 2);
        assertEquals(records.getFirst().eventId(), records.getLast().eventId());
        assertEquals("mock-provider", records.getFirst().provider());
        assertEquals(1, records.getFirst().routeOrder());
        assertEquals("DELIVERED", records.getFirst().code());
        assertNotNull(records.getFirst().receivedAt());
    }

    @Test
    void brokerRejectedWriteReturns503AndSameReceiptCanRecover() throws Exception {
        String body = ReceiptHttpTest.body();
        try {
            setMaxMessageBytes("100"); // Reject this topic's batches only; never stop the shared broker.
            var failed = post(body);
            assertEquals(503, failed.statusCode());
            assertEquals("RECEIPT_UNCONFIRMED", mapper.readTree(failed.body()).get("code").asText());
        } finally {
            setMaxMessageBytes("1048588");
        }
        assertEquals(202, post(body).statusCode());
        assertEquals(mapper.readTree(body).get("receiptId").asText(),
                read(mapper.readTree(body).get("deliveryId").asText(), 1).getFirst().receiptId());
    }

    private static Admin admin() {
        return Admin.create(Map.of("bootstrap.servers", bootstrap(), "request.timeout.ms", 5000, "default.api.timeout.ms", 10000));
    }

    private void setMaxMessageBytes(String value) throws Exception {
        try (var admin = admin()) {
            var resource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC);
            admin.incrementalAlterConfigs(Map.of(resource,
                    List.of(new AlterConfigOp(new ConfigEntry("max.message.bytes", value), AlterConfigOp.OpType.SET))))
                    .all().get(10, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (System.nanoTime() < deadline) {
                var actual = admin.describeConfigs(List.of(resource)).all().get(5, TimeUnit.SECONDS).get(resource);
                if (value.equals(actual.get("max.message.bytes").value())) return;
                Thread.sleep(20);
            }
            fail("Test topic configuration was not applied");
        }
    }

    private HttpResponse<String> post(String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + environment.getProperty("local.server.port")
                        + "/api/v1/receipts/mock-provider"))
                .timeout(Duration.ofSeconds(15)).header("Authorization", "Bearer " + ReceiptHttpTest.PRIMARY)
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build();
        try (var client = HttpClient.newHttpClient()) { return client.send(request, HttpResponse.BodyHandlers.ofString()); }
    }

    private List<ReceiptEvent> read(String deliveryId, int count) {
        try (var consumer = new KafkaConsumer<>(Map.of("bootstrap.servers", bootstrap(), "enable.auto.commit", false),
                new StringDeserializer(), new StringDeserializer())) {
            var partitions = List.of(new TopicPartition(TOPIC, 0), new TopicPartition(TOPIC, 1), new TopicPartition(TOPIC, 2));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            var found = new ArrayList<ReceiptEvent>();
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(100))) {
                    if (!deliveryId.equals(record.key())) continue;
                    assertNull(record.headers().lastHeader("__TypeId__"), "Kafka contracts must not depend on Java class names");
                    found.add(mapper.readValue(record.value(), ReceiptEvent.class));
                }
                if (found.size() >= count) return found;
            }
            throw new AssertionError("Missing receipt records: expected=" + count + " observed=" + found.size());
        }
    }
}

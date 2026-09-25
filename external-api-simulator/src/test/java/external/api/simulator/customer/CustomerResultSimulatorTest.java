package external.api.simulator.customer;

import event.common.lifecycle.DeliveryFinalized;
import event.common.notification.CustomerResultBatch;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import tools.jackson.databind.json.JsonMapper;
import external.api.simulator.MockDeliveryProviderApplication;
import org.springframework.boot.SpringApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class CustomerResultSimulatorTest {
    static final String SECRET = "customer-result-simulator-secret-1234";
    final CustomerResultSimulator simulator = new CustomerResultSimulator(SECRET, 100, 1);
    final JsonMapper mapper = JsonMapper.builder().build();
    CustomerResultBatch batch() {
        String id = UUID.randomUUID().toString();
        Instant now = Instant.now();
        return new CustomerResultBatch(1, UUID.randomUUID().toString(), 42, List.of(new DeliveryFinalized(1, "DeliveryFinalized", DeliveryFinalized.eventId(id), id, 42, "RCS", "DELIVERED", "DELIVERED", 1, UUID.randomUUID().toString(), "mock-provider", now, now, now, now)));
    }
    MockHttpServletRequest request(CustomerResultBatch batch) {
        var request = new MockHttpServletRequest();
        request.addHeader("Authorization", "Bearer " + SECRET); request.addHeader("Idempotency-Key", batch.batchId());
        request.setContent(mapper.writeValueAsBytes(batch)); return request;
    }
    @Test void failureOnceThenAcknowledgementKeepsOneCustomerEffect() throws Exception {
        var batch = batch();
        assertEquals(503, simulator.receive(42, CustomerResultSimulator.Mode.FAIL_ONCE, request(batch)).getStatusCode().value());
        assertEquals(204, simulator.receive(42, CustomerResultSimulator.Mode.FAIL_ONCE, request(batch)).getStatusCode().value());
        assertEquals(204, simulator.receive(42, CustomerResultSimulator.Mode.ACK, request(batch)).getStatusCode().value());
        assertEquals(3, simulator.summary().get("requests")); assertEquals(1, simulator.summary().get("uniqueResultEffects"));
    }
    @Test void effectBeforeFailedAcknowledgementRemainsDeduplicated() throws Exception {
        var batch = batch();
        for (int i = 0; i < 2; i++) assertEquals(503, simulator.receive(42, CustomerResultSimulator.Mode.ACK_THEN_503, request(batch)).getStatusCode().value());
        assertEquals(2, simulator.summary().get("requests")); assertEquals(1, simulator.summary().get("uniqueResultEffects"));
    }
    @Test void unauthorizedMismatchedTenantAndChangedBodyAreRejected() throws Exception {
        var batch = batch(); var unauthorized = request(batch); unauthorized.removeHeader("Authorization");
        assertEquals(401, simulator.receive(42, CustomerResultSimulator.Mode.ACK, unauthorized).getStatusCode().value());
        assertEquals(400, simulator.receive(43, CustomerResultSimulator.Mode.ACK, request(batch)).getStatusCode().value());
        assertEquals(204, simulator.receive(42, CustomerResultSimulator.Mode.ACK, request(batch)).getStatusCode().value());
        var changed = new CustomerResultBatch(1, batch.batchId(), 42, batch().results());
        assertEquals(409, simulator.receive(42, CustomerResultSimulator.Mode.ACK, request(changed)).getStatusCode().value());
        assertEquals(1, simulator.summary().get("requests"));
    }
    @Test void oversizedAndOverCountBodiesDoNotRecordEffects() throws Exception {
        var batch = batch(); var oversized = request(batch); oversized.setContent(new byte[1_048_577]);
        assertEquals(413, simulator.receive(42, CustomerResultSimulator.Mode.ACK, oversized).getStatusCode().value());
        var repeated = new CustomerResultBatch(1, batch.batchId(), 42, java.util.Collections.nCopies(101, batch.results().getFirst()));
        assertEquals(400, simulator.receive(42, CustomerResultSimulator.Mode.ACK, request(repeated)).getStatusCode().value());
        assertEquals(0, simulator.summary().get("requests"));
    }

    @Test void actualApplicationServesAuthenticatedHttpAndLoopbackObservation() throws Exception {
        try (var app = SpringApplication.run(MockDeliveryProviderApplication.class,
                "--server.address=127.0.0.1", "--server.port=0", "--management.server.port=0",
                "--simulator.tcp.enabled=false", "--simulator.receipts.enabled=false",
                "--simulator.customer-results.enabled=true", "--simulator.customer-results.secret=" + SECRET);
             var http = HttpClient.newHttpClient()) {
            int port = app.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
            int management = app.getEnvironment().getRequiredProperty("local.management.port", Integer.class);
            var batch = batch();
            var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/v1/customer-results/42/FAIL_ONCE"))
                    .header("Authorization", "Bearer " + SECRET).header("Idempotency-Key", batch.batchId())
                    .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(batch))).build();
            assertEquals(503, http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode());
            assertEquals(204, http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode());
            var observed = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + management + "/actuator/customerResults")).GET().build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, observed.statusCode());
            assertEquals(2, mapper.readTree(observed.body()).get("requests").asInt());
            assertEquals(1, mapper.readTree(observed.body()).get("uniqueResultEffects").asInt());
        }
    }
}

package messaging.webhook.sender;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CustomerWebhookClientTest {
    @Test
    void postsStableIdAndTreatsOnly204AsAcknowledged() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var status = new AtomicInteger(503);
        var seenId = new AtomicReference<String>();
        var seenBody = new AtomicReference<String>();
        var seenAuth = new AtomicReference<String>();
        server.createContext("/webhook", exchange -> {
            seenId.set(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
            seenAuth.set(exchange.getRequestHeaders().getFirst("Authorization"));
            seenBody.set(new String(exchange.getRequestBody().readAllBytes()));
            exchange.sendResponseHeaders(status.get(), -1);
            exchange.close();
        });
        server.start();
        try {
            var destination = new WebhookSenderProperties.Destination(
                    URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/webhook"));
            var settings = new WebhookSenderProperties(true, 100, 1, Duration.ZERO, 262144,
                    Duration.ofSeconds(3), Duration.ofSeconds(30), Duration.ofSeconds(1),
                    Duration.ofSeconds(60), Map.of(42L, destination));
            var claim = new WebhookBatchRepository.Claim(UUID.randomUUID(), 42L,
                    destination.url().toString(), "{\"results\":[1]}", 1, 1, UUID.randomUUID());
            try (var client = new CustomerWebhookClient(settings)) {
                var failed = client.send(claim, destination);
                assertFalse(failed.acknowledged());
                assertEquals(503, failed.httpStatus());
                status.set(204);
                var acknowledged = client.send(claim, destination);
                assertTrue(acknowledged.acknowledged());
                assertEquals(204, acknowledged.httpStatus());
            }
            assertEquals(claim.batchId().toString(), seenId.get());
            assertEquals(claim.body(), seenBody.get());
            assertNull(seenAuth.get());
        } finally { server.stop(0); }
    }
}

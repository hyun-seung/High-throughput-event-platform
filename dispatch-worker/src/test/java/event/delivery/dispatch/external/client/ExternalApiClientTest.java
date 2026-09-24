package event.delivery.dispatch.external.client;

import com.sun.net.httpserver.HttpServer;
import event.common.delivery.DeliveryEvent;
import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.external.config.ExternalApiClientConfig;
import event.delivery.dispatch.external.config.ExternalApiProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.web.client.RestClient;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static event.delivery.dispatch.external.client.ProviderFailureException.Kind.*;
import static org.junit.jupiter.api.Assertions.*;

class ExternalApiClientTest {
    private final DeliveryEvent event = DeliveryEvent.requested("delivery-1", 1L, "SMS", Map.of(),
            Instant.parse("2026-09-24T00:00:00Z"));
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicReference<String> receivedKey = new AtomicReference<>();
    private final CountDownLatch releaseResponse = new CountDownLatch(1);
    private HttpServer server;
    private ExecutorService executor;
    private ExternalApiClient client;
    private volatile int status = 200;
    private volatile String body;
    private volatile boolean holdResponse;
    private volatile boolean holdBody;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(executor);
        server.createContext("/api/v1/deliveries", exchange -> {
            try (exchange) {
                calls.incrementAndGet();
                receivedKey.set(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
                exchange.getRequestBody().readAllBytes();
                if (holdResponse) {
                    try {
                        releaseResponse.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
                if (holdBody) {
                    exchange.getResponseBody().write(bytes, 0, 1);
                    exchange.getResponseBody().flush();
                    try {
                        releaseResponse.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    exchange.getResponseBody().write(bytes, 1, bytes.length - 1);
                    return;
                }
                exchange.getResponseBody().write(bytes);
            }
        });
        server.start();
        var properties = new ExternalApiProperties("http://127.0.0.1:" + server.getAddress().getPort(),
                Duration.ofSeconds(2), Duration.ofMillis(500));
        client = new ExternalApiClient(new ExternalApiClientConfig().externalApiRestClient(RestClient.builder(), properties),
                new DispatchProperties(null, null));
    }

    @AfterEach
    void stop() {
        releaseResponse.countDown();
        if (server != null) server.stop(0);
        if (executor != null) executor.close();
    }

    private String response(String id, String accepted, String code) {
        return "{\"deliveryId\":\"%s\",\"accepted\":%s,\"processedAt\":\"2026-09-24T00:00:01Z\",\"code\":%s}"
                .formatted(id, accepted, code == null ? "null" : "\"" + code + "\"");
    }

    @Test
    void acceptedResponsePreservesIdentityAndSendsAttemptKey() {
        body = response(event.deliveryId(), "true", "ACCEPTED");
        var result = client.send(event, "attempt-1");
        assertEquals(event.deliveryId(), result.deliveryId());
        assertTrue(result.accepted());
        assertEquals(Instant.parse("2026-09-24T00:00:01Z"), result.processedAt());
        assertEquals("attempt-1", receivedKey.get());
        assertEquals(1, calls.get());
    }

    @Test
    void originalSimulatorSuccessWithoutCodeRemainsCompatible() {
        body = "{\"deliveryId\":\"delivery-1\",\"accepted\":true,\"processedAt\":\"2026-09-24T00:00:01Z\"}";
        assertTrue(client.send(event, "attempt-1").accepted());
    }

    @ParameterizedTest
    @CsvSource({"200,RETRY_1S,RETRY_1S", "429,RETRY_1S,RETRY_1S", "429,RETRY_10S,RETRY_10S",
            "503,FALLBACK,FALLBACK_REQUIRED", "400,REJECTED,PERMANENT_REJECTION"})
    void businessCodesAreClassifiedIncludingHttpErrorBodies(int httpStatus, String code, ProviderFailureException.Kind kind) {
        status = httpStatus;
        body = response(event.deliveryId(), "false", code);
        var failure = assertThrows(ProviderFailureException.class, () -> client.send(event, "attempt-1"));
        assertEquals(kind, failure.kind());
        assertEquals(1, calls.get(), "Classification must not automatically re-send");
    }

    @ParameterizedTest
    @CsvSource({"200,another-delivery,true,ACCEPTED", "500,delivery-1,true,ACCEPTED",
            "200,delivery-1,false,ACCEPTED", "200,delivery-1,true,RETRY_1S",
            "503,delivery-1,false,UNRECOGNIZED", "200,delivery-1,null,RETRY_1S"})
    void contradictoryOrUnknownResponsesDoNotBecomeSuccessOrRetry(int httpStatus, String id, String accepted, String code) {
        status = httpStatus;
        body = response(id, accepted, code);
        assertEquals(INVALID_RESPONSE, assertThrows(ProviderFailureException.class,
                () -> client.send(event, "attempt-1")).kind());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "private-provider-body-not-json", "{}", "{\"deliveryId\":\"delivery-1\",\"accepted\":true}"})
    void malformedOrIncompleteResponseIsSanitized(String wireBody) {
        body = wireBody;
        var failure = assertThrows(ProviderFailureException.class, () -> client.send(event, "attempt-1"));
        assertEquals(INVALID_RESPONSE, failure.kind());
        assertEquals("Provider outcome: INVALID_RESPONSE", failure.getMessage());
        assertNull(failure.getCause(), "Raw provider response must not leak through Kafka error stack traces");
    }

    @Test
    void legacyForceFailDoesNotInferRetryFromHttp500Alone() {
        status = 500;
        body = response(event.deliveryId(), "false", null);
        assertEquals(HTTP_ERROR, assertThrows(ProviderFailureException.class,
                () -> client.send(event, "attempt-1")).kind());
    }

    @Test
    void observedReadTimeoutIsSeparateFromProtocolFailure() {
        body = response(event.deliveryId(), "true", "ACCEPTED");
        holdResponse = true;
        assertEquals(NO_RESPONSE, assertThrows(ProviderFailureException.class,
                () -> client.send(event, "attempt-1")).kind());
        assertEquals(1, calls.get());
    }

    @Test
    void timeoutDuringBodyExtractionIsAlsoAnObservedTransportFailure() {
        body = response(event.deliveryId(), "true", "ACCEPTED");
        holdBody = true;
        assertEquals(NO_RESPONSE, assertThrows(ProviderFailureException.class,
                () -> client.send(event, "attempt-1")).kind());
        assertEquals(1, calls.get());
    }
}

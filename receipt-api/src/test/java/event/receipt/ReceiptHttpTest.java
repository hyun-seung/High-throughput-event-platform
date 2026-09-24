package event.receipt;

import event.common.receipt.ReceiptEvent;
import event.receipt.kafka.ReceiptPublisher;
import event.receipt.config.ReceiptProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "management.server.port=0", "spring.kafka.admin.auto-create=false", "spring.mvc.async.request-timeout=100ms",
        "receipt.primary-secret=primary-test-secret-with-more-than-32-characters",
        "receipt.secondary-secret=secondary-test-secret-with-more-than-32-characters"})
class ReceiptHttpTest {
    static final String PRIMARY = "primary-test-secret-with-more-than-32-characters";
    static final String SECONDARY = "secondary-test-secret-with-more-than-32-characters";
    @MockitoBean ReceiptPublisher publisher;
    @Autowired Environment environment;
    @Autowired JsonMapper mapper;

    @BeforeEach
    void resetPublisher() { reset(publisher); when(publisher.publish(any())).thenReturn(CompletableFuture.completedFuture(null)); }

    @Test
    void respondsOnlyAfterAckAndDerivesProviderAndRouteFromAuthentication() throws Exception {
        var ack = new CompletableFuture<Void>();
        when(publisher.publish(any())).thenReturn(ack);
        Instant beforeRequest = Instant.now();
        try (var client = HttpClient.newHttpClient()) {
            var response = client.sendAsync(request("tcp-provider", SECONDARY, body()), HttpResponse.BodyHandlers.ofString());
            verify(publisher, timeout(5000)).publish(any());
            assertFalse(response.isDone(), "HTTP must not acknowledge an unconfirmed Kafka write");
            ack.complete(null);
            var accepted = response.get(5, TimeUnit.SECONDS);
            assertEquals(202, accepted.statusCode());
            var event = ArgumentCaptor.forClass(ReceiptEvent.class);
            verify(publisher).publish(event.capture());
            assertEquals("tcp-provider", event.getValue().provider());
            assertEquals(2, event.getValue().routeOrder());
            assertEquals(event.getValue().eventId(), mapper.readTree(accepted.body()).get("eventId").asText());
            assertEquals("RECEIVED", mapper.readTree(accepted.body()).get("status").asText());
            assertFalse(event.getValue().receivedAt().isBefore(beforeRequest));
            assertFalse(event.getValue().receivedAt().isAfter(Instant.now()));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "wrong-key", SECONDARY})
    void missingWrongAndOtherProviderCredentialsCannotPublish(String token) throws Exception {
        assertEquals(401, send(request("mock-provider", token, body())).statusCode());
        verifyNoInteractions(publisher);
    }

    @Test
    void unknownProviderCannotPublishWithValidCredential() throws Exception {
        assertEquals(401, send(request("unknown", PRIMARY, body())).statusCode());
        verifyNoInteractions(publisher);
    }

    @Test
    void duplicateAuthorizationHeadersAreRejected() throws Exception {
        var req = HttpRequest.newBuilder(uri("mock-provider")).header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + PRIMARY).header("Authorization", "Bearer " + PRIMARY)
                .POST(HttpRequest.BodyPublishers.ofString(body())).build();
        assertEquals(401, send(req).statusCode());
        verifyNoInteractions(publisher);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{bad-json", "{\"receiptId\":\"bad\\nvalue\"}",
            "{\"outcome\":\"ACCEPTED\"}"})
    void invalidBodiesNeverReachKafka(String body) throws Exception {
        assertEquals(400, send(request("mock-provider", PRIMARY, body)).statusCode());
        verifyNoInteractions(publisher);
    }

    @Test
    void rejectsUnknownFieldsAndContradictoryCodes() throws Exception {
        assertEquals(400, send(request("mock-provider", PRIMARY, body().replace("\"DELIVERED\",\"occurredAt\"", "\"RECEIVED\",\"occurredAt\""))).statusCode());
        assertEquals(400, send(request("mock-provider", PRIMARY, body().replace("{", "{\"provider\":\"tcp-provider\","))).statusCode());
        verifyNoInteractions(publisher);
    }

    @Test
    void preservesUnknownFailureCodesForLaterPolicyHandling() throws Exception {
        String body = body().replace("\"outcome\":\"DELIVERED\"", "\"outcome\":\"FAILED\"")
                .replace("\"code\":\"DELIVERED\"", "\"code\":\"NEW_PROVIDER_CODE\"");
        assertEquals(202, send(request("mock-provider", PRIMARY, body)).statusCode());
        var captured = ArgumentCaptor.forClass(ReceiptEvent.class);
        verify(publisher).publish(captured.capture());
        assertEquals("NEW_PROVIDER_CODE", captured.getValue().code());
    }

    @Test
    void invocationIsPreservedAndOutOfRangeInvocationCannotPublish() throws Exception {
        assertEquals(202, send(request("mock-provider", PRIMARY, body().replace("{", "{\"invocation\":3,"))).statusCode());
        var captured = ArgumentCaptor.forClass(ReceiptEvent.class);
        verify(publisher).publish(captured.capture());
        assertEquals(3, captured.getValue().invocation());
        clearInvocations(publisher);
        assertEquals(400, send(request("mock-provider", PRIMARY, body().replace("{", "{\"invocation\":5,"))).statusCode());
        verifyNoInteractions(publisher);
    }

    @Test
    void limitsBodyWithAndWithoutContentLength() throws Exception {
        byte[] oversized = (" ".repeat(17000) + body()).getBytes(StandardCharsets.UTF_8);
        assertEquals(413, send(request("mock-provider", PRIMARY, new String(oversized, StandardCharsets.UTF_8))).statusCode());
        var chunked = HttpRequest.newBuilder(uri("mock-provider")).header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + PRIMARY)
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(oversized))).build();
        assertEquals(413, send(chunked).statusCode());
        verifyNoInteractions(publisher);
    }

    @Test
    void synchronousAndAsynchronousKafkaFailuresReturnSafeRetryContract() throws Exception {
        when(publisher.publish(any())).thenThrow(new IllegalStateException("internal-secret-information"));
        assertUnconfirmed(send(request("mock-provider", PRIMARY, body())));
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("internal-secret-information"))).when(publisher).publish(any());
        assertUnconfirmed(send(request("mock-provider", PRIMARY, body())));
    }

    @Test
    void httpTimeoutDoesNotCancelPublishAndDuplicateRetryKeepsIdentity() throws Exception {
        String body = body();
        var ack = new CompletableFuture<Void>();
        when(publisher.publish(any())).thenReturn(ack);
        assertUnconfirmed(send(request("mock-provider", PRIMARY, body)));
        assertFalse(ack.isCancelled());
        ack.complete(null); // The broker may store the receipt after HTTP has already timed out.
        when(publisher.publish(any())).thenReturn(CompletableFuture.completedFuture(null));
        assertEquals(202, send(request("mock-provider", PRIMARY, body)).statusCode());
        var captured = ArgumentCaptor.forClass(ReceiptEvent.class);
        verify(publisher, times(2)).publish(captured.capture());
        assertEquals(captured.getAllValues().getFirst().eventId(), captured.getAllValues().getLast().eventId());
    }

    @Test
    void emptySecretsDisableProvidersAndUnsafeConfigurationFailsClosed() {
        var disabled = new ReceiptProperties(null, null, null, null);
        assertEquals("", disabled.secret("mock-provider"));
        assertEquals("", disabled.secret("tcp-provider"));
        assertThrows(IllegalArgumentException.class, () -> new ReceiptProperties(null, null, "short", null));
        assertThrows(IllegalArgumentException.class, () -> new ReceiptProperties(null, null, PRIMARY, PRIMARY));
        assertThrows(IllegalArgumentException.class, () -> new ReceiptProperties("same", "same", PRIMARY, SECONDARY));
    }

    private void assertUnconfirmed(HttpResponse<String> result) {
        assertEquals(503, result.statusCode());
        assertEquals("RECEIPT_UNCONFIRMED", mapper.readTree(result.body()).get("code").asText());
        assertFalse(result.body().contains("internal-secret"));
    }

    private URI uri(String provider) {
        return URI.create("http://127.0.0.1:" + environment.getProperty("local.server.port") + "/api/v1/receipts/" + provider);
    }

    private HttpRequest request(String provider, String token, String body) {
        var builder = HttpRequest.newBuilder(uri(provider)).timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json");
        if (!token.isEmpty()) builder.header("Authorization", "Bearer " + token);
        return builder.POST(HttpRequest.BodyPublishers.ofString(body)).build();
    }

    private HttpResponse<String> send(HttpRequest request) throws Exception {
        try (var client = HttpClient.newHttpClient()) { return client.send(request, HttpResponse.BodyHandlers.ofString()); }
    }

    static String body() {
        return "{\"receiptId\":\"receipt-" + UUID.randomUUID() + "\",\"deliveryId\":\"" + UUID.randomUUID() + "\",\"attemptId\":\"" + UUID.randomUUID()
                + "\",\"outcome\":\"DELIVERED\",\"code\":\"DELIVERED\",\"occurredAt\":\"2026-09-24T00:00:00Z\"}";
    }
}

package messaging.api.messages;

import com.sun.net.httpserver.HttpServer;
import messaging.common.dynamodb.DynamoDbAutoConfiguration;
import messaging.common.dynamodb.DynamoDbProperties;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageSubmission;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import io.micrometer.core.instrument.MeterRegistry;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Real production SDK/store against a local server modeling an ambiguous conditional Put. */
class MessageReceiveSdkRetryTest {
    record Call(String target, JsonNode body) { }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void committedPutThenRetryConflictReusesOriginForKafkaPublication(boolean disconnect) throws Exception {
        var mapper = JsonMapper.builder().build();
        var calls = new CopyOnWriteArrayList<Call>();
        var stored = new AtomicReference<JsonNode>();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                String target = exchange.getRequestHeaders().getFirst("X-Amz-Target");
                JsonNode body = mapper.readTree(exchange.getRequestBody().readAllBytes());
                calls.add(new Call(target, body));
                int status;
                String response;
                if (target.endsWith(".PutItem")) {
                    if (stored.compareAndSet(null, body.get("Item"))) {
                        // Storage committed, but the client cannot confirm this first attempt.
                        if (disconnect) return;
                        status = 500;
                        response = "{\"__type\":\"InternalServerError\",\"message\":\"response unconfirmed\"}";
                    } else {
                        status = 400;
                        response = "{\"__type\":\"ConditionalCheckFailedException\",\"message\":\"already stored\"}";
                    }
                } else if (target.endsWith(".GetItem")) {
                    status = 200;
                    response = mapper.writeValueAsString(Map.of("Item", stored.get()));
                } else {
                    status = 400;
                    response = "{\"__type\":\"UnknownOperationException\"}";
                }
                byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.0");
                exchange.sendResponseHeaders(status, bytes.length);
                exchange.getResponseBody().write(bytes);
            }
        });
        server.start();
        try {
            var properties = new DynamoDbProperties();
            properties.setEndpoint("http://127.0.0.1:" + server.getAddress().getPort());
            var beans = new StaticListableBeanFactory();
            try (var db = new DynamoDbAutoConfiguration().dynamoDbClient(properties,
                    beans.getBeanProvider(MeterRegistry.class))) {
                var usage = mock(MessageUsageLimiter.class);
                var duplicates = mock(MessageDuplicateGuard.class);
                var publisher = mock(MessageRequestPublisher.class);
                var claim = new MessageDuplicateGuard.Claim("stable-id", "key", "value", true, true);
                when(duplicates.claim(eq(42L), any(), anyString())).thenReturn(claim);
                var request = new MessageReceiveRequest("customer-1", "01012345678", MessageCategory.GENERAL,
                        Map.of("message", "hello"), Map.of("message", "fallback"));
                var original = new MessageSubmission(claim.clientMsgId(), 42L, request.messageId(),
                        request.recipientNumber(), request.messageCategory(), request.payload(),
                        request.secondarySendPayload(), Instant.parse("2026-10-02T00:00:00Z"));
                var ack = new CompletableFuture<Void>();
                when(publisher.publish(original)).thenAnswer(call -> {
                    assertFalse(ack.isDone());
                    ack.complete(null);
                    return ack;
                });
                var service = new MessageReceiveService(usage, duplicates, new DynamoMessageOriginStore(db, mapper),
                        publisher, Clock.fixed(original.receivedAt(), ZoneOffset.UTC));

                assertEquals(claim.clientMsgId(), service.receive(42L, request).clientMsgId());

                assertTrue(ack.isDone());
                verify(publisher).publish(original);
                verify(duplicates, never()).release(any());
                assertEquals(List.of("DynamoDB_20120810.PutItem", "DynamoDB_20120810.PutItem",
                        "DynamoDB_20120810.GetItem"), calls.stream().map(Call::target).toList());
                assertEquals(calls.get(0).body(), calls.get(1).body());
                assertEquals("attribute_not_exists(pk) AND attribute_not_exists(sk)",
                        calls.get(0).body().get("ConditionExpression").asString());
                assertTrue(calls.get(2).body().get("ConsistentRead").asBoolean());
                assertEquals("DELIVERY#stable-id", calls.get(2).body().get("Key").get("pk").get("S").asString());
            }
        } finally {
            server.stop(0);
        }
    }
}

package messaging.tcp.sender;

import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.PrimaryStageDecision;
import messaging.common.messages.SecondarySendCommand;
import messaging.common.messages.TcpSendResult;
import messaging.common.tcp.TcpDeliveryRequest;
import messaging.common.tcp.TcpDeliveryResponse;
import messaging.common.tcp.TcpFrames;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.net.ServerSocket;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class TcpProviderClientTest {
    private static final Instant NOW = Instant.parse("2026-10-07T12:00:00Z");
    private static final String ID = "a".repeat(32);
    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void acceptedImmediateReplyBecomesSuccess() throws Exception {
        var command = command();
        try (var server = new ServerSocket(0)) {
            var exchange = CompletableFuture.supplyAsync(() -> {
                try (var socket = server.accept()) {
                    var request = mapper.readValue(TcpFrames.read(socket.getInputStream()), TcpDeliveryRequest.class);
                    TcpFrames.write(socket.getOutputStream(), mapper.writeValueAsBytes(new TcpDeliveryResponse(
                            request.deliveryId(), request.attemptId(), true, NOW, "RECEIVED")));
                    return request;
                } catch (Exception failure) { throw new RuntimeException(failure); }
            });
            var result = client(server.getLocalPort()).send(command);
            assertEquals(TcpSendResult.Status.SUCCESS, result.status());
            assertNull(result.errorCode());
            assertEquals(ID, exchange.get().deliveryId());
            assertEquals(command.attemptId(), exchange.get().attemptId());
            assertEquals(command.submission().secondarySendPayload(), exchange.get().payload());
        }
    }

    @Test
    void rejectionRetainsProviderCodeAndUsesSecondaryErrorRange() throws Exception {
        var command = command();
        try (var server = new ServerSocket(0)) {
            var exchange = CompletableFuture.runAsync(() -> {
                try (var socket = server.accept()) {
                    var request = mapper.readValue(TcpFrames.read(socket.getInputStream()), TcpDeliveryRequest.class);
                    TcpFrames.write(socket.getOutputStream(), mapper.writeValueAsBytes(new TcpDeliveryResponse(
                            request.deliveryId(), request.attemptId(), false, NOW, "REJECTED")));
                } catch (Exception failure) { throw new RuntimeException(failure); }
            });
            var result = client(server.getLocalPort()).send(command);
            exchange.get();
            assertEquals(TcpSendResult.Status.FAILED, result.status());
            assertEquals(70001, result.errorCode());
            assertEquals("REJECTED", result.providerCode());
        }
    }

    @Test
    void mismatchedAttemptCannotBeAccepted() throws Exception {
        var command = command();
        try (var server = new ServerSocket(0)) {
            var exchange = CompletableFuture.runAsync(() -> {
                try (var socket = server.accept()) {
                    TcpFrames.read(socket.getInputStream());
                    TcpFrames.write(socket.getOutputStream(), mapper.writeValueAsBytes(new TcpDeliveryResponse(
                            ID, "another-attempt", true, NOW, "RECEIVED")));
                } catch (Exception failure) { throw new RuntimeException(failure); }
            });
            var result = client(server.getLocalPort()).send(command);
            exchange.get();
            assertEquals(TcpSendResult.Status.INVALID_RESPONSE, result.status());
            assertEquals(40005, result.errorCode());
        }
    }

    private TcpProviderClient client(int port) {
        return new TcpProviderClient(new TcpSenderProperties(true, "127.0.0.1", port,
                Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(30)),
                mapper, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static SecondarySendCommand command() {
        var decision = new PrimaryStageDecision("primary-failed", ID, PrimaryStageDecision.Kind.FAILURE,
                "HTTP_RESPONSE", 66003, null, null, null, true, NOW);
        var submission = new MessageSubmission(ID, 42L, "customer-1", "01012345678",
                MessageCategory.GENERAL, Map.of("text", "primary"), Map.of("text", "secondary"), NOW);
        return SecondarySendCommand.from(decision, submission);
    }
}

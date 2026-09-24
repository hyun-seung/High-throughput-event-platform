package event.delivery.dispatch.external.client;

import event.common.delivery.DeliveryEvent;
import event.common.tcp.TcpDeliveryRequest;
import event.common.tcp.TcpDeliveryResponse;
import event.common.tcp.TcpFrames;
import event.delivery.dispatch.external.config.TcpProviderProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static event.delivery.dispatch.external.client.ProviderFailureException.Kind.*;
import static org.junit.jupiter.api.Assertions.*;

class TcpProviderClientTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final DeliveryEvent event = DeliveryEvent.requested("delivery", 1L, "SMS", Map.of("text", "test"), Instant.now(), true);

    @Test
    void receiptRequiresCorrelatedBusinessReplyAndSupportsFragmentedFrame() throws Exception {
        exchange(socket -> {
            var request = mapper.readValue(TcpFrames.read(socket.getInputStream()), TcpDeliveryRequest.class);
            assertEquals(event.deliveryId(), request.deliveryId());
            assertEquals("attempt", request.attemptId());
            assertEquals(3, request.invocation());
            byte[] reply = mapper.writeValueAsBytes(new TcpDeliveryResponse("delivery", "attempt", true, Instant.now(), "RECEIVED"));
            var buffer = new ByteArrayOutputStream();
            TcpFrames.write(buffer, reply);
            for (byte b : buffer.toByteArray()) {
                socket.getOutputStream().write(b);
                socket.getOutputStream().flush();
            }
        }, client -> assertTrue(client.send(event, "attempt", 3).accepted()));
    }

    @ParameterizedTest
    @CsvSource({"other,attempt,true,RECEIVED,INVALID_RESPONSE", "delivery,other,true,RECEIVED,INVALID_RESPONSE",
            "delivery,attempt,true,REJECTED,INVALID_RESPONSE", "delivery,attempt,false,RETRY_1S,RETRY_1S",
            "delivery,attempt,false,RETRY_10S,RETRY_10S", "delivery,attempt,false,REJECTED,PERMANENT_REJECTION",
            "delivery,attempt,false,RECEIVED,INVALID_RESPONSE", "delivery,attempt,true,UNKNOWN,INVALID_RESPONSE"})
    void responseCodesAndIdentityAreChecked(String deliveryId, String attemptId, boolean accepted, String code,
                                             ProviderFailureException.Kind expected) throws Exception {
        exchange(socket -> {
            TcpFrames.read(socket.getInputStream());
            TcpFrames.write(socket.getOutputStream(), mapper.writeValueAsBytes(
                    new TcpDeliveryResponse(deliveryId, attemptId, accepted, Instant.now(), code)));
        }, client -> assertEquals(expected, assertThrows(ProviderFailureException.class, () -> client.send(event, "attempt")).kind()));
    }

    @Test
    void connectionAndWriteSuccessWithoutReplyRemainNoResponse() throws Exception {
        exchange(socket -> TcpFrames.read(socket.getInputStream()),
                client -> assertEquals(NO_RESPONSE, assertThrows(ProviderFailureException.class, () -> client.send(event, "attempt")).kind()));
    }

    @Test
    void truncatedReplyCannotBecomeAcceptance() throws Exception {
        exchange(socket -> {
            TcpFrames.read(socket.getInputStream());
            new DataOutputStream(socket.getOutputStream()).writeInt(100);
            socket.getOutputStream().write('{');
        }, client -> assertEquals(NO_RESPONSE, assertThrows(ProviderFailureException.class, () -> client.send(event, "attempt")).kind()));
    }

    @Test
    void oversizedDeclaredReplyIsRejectedBeforeAllocation() throws Exception {
        exchange(socket -> {
            TcpFrames.read(socket.getInputStream());
            new DataOutputStream(socket.getOutputStream()).writeInt(TcpFrames.MAX_BYTES + 1);
        }, client -> assertEquals(INVALID_RESPONSE, assertThrows(ProviderFailureException.class, () -> client.send(event, "attempt")).kind()));
    }

    @Test
    void exchangeDeadlineClosesStalledConnection() throws Exception {
        var release = new CountDownLatch(1);
        try {
            exchange(socket -> {
                TcpFrames.read(socket.getInputStream());
                release.await(2, TimeUnit.SECONDS);
            }, client -> {
                try {
                    assertEquals(NO_RESPONSE, assertThrows(ProviderFailureException.class, () -> client.send(event, "attempt")).kind());
                } finally { release.countDown(); }
            });
        } finally { release.countDown(); }
    }

    private void exchange(Peer peer, java.util.function.Consumer<TcpProviderClient> action) throws Exception {
        try (var server = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress());
             var pool = Executors.newVirtualThreadPerTaskExecutor();
             var client = new TcpProviderClient(new TcpProviderProperties("localhost", server.getLocalPort(),
                     Duration.ofSeconds(1), Duration.ofMillis(500)), mapper)) {
            var response = pool.submit(() -> {
                try (var socket = server.accept()) {
                    socket.setSoTimeout(2000);
                    peer.handle(socket);
                }
                return null;
            });
            action.accept(client);
            response.get(3, TimeUnit.SECONDS);
        }
    }

    @FunctionalInterface interface Peer { void handle(Socket socket) throws Exception; }
}

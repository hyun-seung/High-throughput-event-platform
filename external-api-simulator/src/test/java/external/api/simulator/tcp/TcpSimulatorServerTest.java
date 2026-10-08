package external.api.simulator.tcp;

import messaging.common.tcp.TcpFrames;
import messaging.common.tcp.TcpDeliveryRequest;
import messaging.common.tcp.TcpDeliveryResponse;
import messaging.common.tcp.MessagingTcpRequest;
import messaging.common.tcp.MessagingTcpResponse;
import external.api.simulator.delivery.config.SimulatorProperties;
import external.api.simulator.delivery.service.SimulatorLedger;
import external.api.simulator.receipt.SimulatorReceiptSender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.io.EOFException;
import java.net.Socket;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TcpSimulatorServerTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final SimulatorLedger ledger = new SimulatorLedger(new SimulatorProperties(0, false, 100), new SimpleMeterRegistry());

    @Test
    void persistentConnectionAcceptsMultipleFrames() throws Exception {
        try (var server = start(); var socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(2000);
            for (int i = 0; i < 3; i++) {
                TcpFrames.write(socket.getOutputStream(), mapper.writeValueAsBytes(new TcpDeliveryRequest(
                        "delivery", "attempt-" + i, 1L, "SMS", Map.of(), Instant.now())));
                var reply = mapper.readValue(TcpFrames.read(socket.getInputStream()), TcpDeliveryResponse.class);
                assertEquals("attempt-" + i, reply.attemptId());
                assertTrue(reply.accepted());
            }
        }
    }

    @Test
    void secondaryDoesNotInheritPrimaryFailureAndDeduplicationIsOff() throws Exception {
        try (var server = start()) {
            for (int i = 0; i < 2; i++) {
                var reply = send(server, Map.of("simulatorResultCode", "FALLBACK", "forceFail", true));
                assertTrue(reply.accepted());
                assertEquals("RECEIVED", reply.code());
                assertEquals("attempt", reply.attemptId());
            }
            assertEquals(Map.of("calls", 2L, "effects", 2L), ledger.counts("tcp:attempt"));
        }
    }

    @Test
    void lostReplyLeavesVisibleEffectAndWrongAttemptScenarioIsAvailable() throws Exception {
        try (var server = start()) {
            assertThrows(EOFException.class, () -> send(server, Map.of("simulatorTcpMode", "close-after-effect")));
            assertEquals(Map.of("calls", 1L, "effects", 1L), ledger.counts("tcp:attempt"));
            assertEquals("unrelated", send(server, Map.of("simulatorTcpMode", "wrong-attempt")).attemptId());
        }
    }

    @Test
    void tcpRejectionDoesNotCreateAnEffect() throws Exception {
        try (var server = start()) {
            var response = send(server, Map.of("simulatorTcpResultCode", "RETRY_10S"));
            assertFalse(response.accepted());
            assertEquals("RETRY_10S", response.code());
            assertEquals(Map.of("calls", 1L, "effects", 0L), ledger.counts("tcp:attempt"));
        }
    }

    @Test
    void messagingTcpProtocolReturnsFinalSuccessAndSevenTenThousandsFailure() throws Exception {
        try (var server = start()) {
            var success = sendMessaging(server, "a".repeat(32), Map.of("text", "secondary"));
            assertEquals("success", success.status());
            assertNull(success.error());
            assertEquals(Map.of("calls", 1L, "effects", 1L), ledger.counts("tcp:" + "a".repeat(32)));

            var failure = sendMessaging(server, "b".repeat(32), Map.of("simulatorTcpErrorCode", 71234));
            assertEquals("fail", failure.status());
            assertEquals(71234, failure.error().code());
            assertEquals(Map.of("calls", 1L, "effects", 0L), ledger.counts("tcp:" + "b".repeat(32)));
        }
    }

    @Test
    void messagingTcpProtocolCanSimulateLostReplyAndWrongId() throws Exception {
        try (var server = start()) {
            assertThrows(EOFException.class, () -> sendMessaging(server, "c".repeat(32),
                    Map.of("simulatorTcpMode", "close-after-effect")));
            assertEquals(Map.of("calls", 1L, "effects", 1L), ledger.counts("tcp:" + "c".repeat(32)));
            assertEquals("unrelated", sendMessaging(server, "d".repeat(32),
                    Map.of("simulatorTcpMode", "wrong-client-msg-id")).clientMsgId());
        }
    }

    private TcpSimulatorServer start() {
        var server = new TcpSimulatorServer(new TcpSimulatorProperties(true, "127.0.0.1", 0, 10, 2000), ledger, mapper,
                org.mockito.Mockito.mock(SimulatorReceiptSender.class));
        server.start();
        return server;
    }

    private TcpDeliveryResponse send(TcpSimulatorServer server, Map<String, Object> payload) throws Exception {
        try (var socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(2000);
            TcpFrames.write(socket.getOutputStream(), mapper.writeValueAsBytes(new TcpDeliveryRequest("delivery", "attempt", 1L, "SMS", payload, Instant.now())));
            return mapper.readValue(TcpFrames.read(socket.getInputStream()), TcpDeliveryResponse.class);
        }
    }

    private MessagingTcpResponse sendMessaging(TcpSimulatorServer server, String clientMsgId,
                                               Map<String, Object> payload) throws Exception {
        try (var socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(2000);
            TcpFrames.write(socket.getOutputStream(), mapper.writeValueAsBytes(new MessagingTcpRequest(
                    clientMsgId, 1L, "customer-message", "01012345678", "GENERAL", payload, Instant.now())));
            return mapper.readValue(TcpFrames.read(socket.getInputStream()), MessagingTcpResponse.class);
        }
    }
}

package external.api.simulator.tcp;

import event.common.tcp.TcpFrames;
import event.common.tcp.TcpDeliveryRequest;
import event.common.tcp.TcpDeliveryResponse;
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
}

package external.api.simulator.receipt;

import com.sun.net.httpserver.HttpServer;
import external.api.simulator.delivery.config.SimulatorProperties;
import external.api.simulator.delivery.controller.DeliveryProviderController;
import external.api.simulator.delivery.dto.ProviderDispatchRequest;
import external.api.simulator.delivery.service.SimulatorLedger;
import external.api.simulator.tcp.TcpSimulatorProperties;
import external.api.simulator.tcp.TcpSimulatorServer;
import event.common.tcp.TcpDeliveryRequest;
import event.common.tcp.TcpFrames;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.json.JsonMapper;

import java.net.*;
import java.io.EOFException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SimulatorReceiptSenderTest {
    static final String PRIMARY = "primary-test-secret-01234567890123456789", SECONDARY = "secondary-test-secret-01234567890123456789";
    final JsonMapper mapper = JsonMapper.builder().build();
    final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    final BlockingQueue<Captured> requests = new LinkedBlockingQueue<>();
    final AtomicInteger status = new AtomicInteger(202);
    final String delivery = UUID.randomUUID().toString(), attempt = UUID.randomUUID().toString();
    HttpServer server;
    SimulatorReceiptSender sender;

    @BeforeEach void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.add(new Captured(exchange.getRequestURI().getPath(), exchange.getRequestHeaders().getFirst("Authorization"),
                    new String(exchange.getRequestBody().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)));
            exchange.sendResponseHeaders(status.get(), -1); exchange.close();
        });
        server.start();
        sender = new SimulatorReceiptSender(config(true, 100, 20), mapper, metrics);
    }
    @AfterEach void stop() { sender.close(); server.stop(0); metrics.close(); }

    @Test void duplicatesKeepExactBodyAndRepeatedInvocationSchedulesOnce() throws Exception {
        var plan = sender.plan(false, attempt, request(Map.of("simulatorReceiptCopies", 3, "simulatorReceiptDelayMillis", 0), 1));
        sender.accepted(plan); sender.accepted(plan);
        Captured first = take();
        assertEquals(first, take()); assertEquals(first, take());
        assertEquals("Bearer " + PRIMARY, first.auth()); assertEquals("/primary", first.path());
        var json = mapper.readTree(first.body());
        assertEquals(attempt, json.get("attemptId").asText()); assertEquals(1, json.get("invocation").asInt());
        assertEquals("DELIVERED", json.get("outcome").asText());
        awaitCount("acknowledged", 3);
        assertEquals(1, count("scheduled")); assertNull(requests.poll(150, TimeUnit.MILLISECONDS));
    }

    @Test void transientFailureRetriesExactBodyButAuthenticationFailureStops() throws Exception {
        status.set(503);
        sender.accepted(sender.plan(false, attempt, request(Map.of(), 1)));
        var first = take(); assertEquals(first, take()); assertEquals(first, take());
        awaitCount("exhausted", 1); assertEquals(2, count("retry"));
        status.set(401);
        sender.accepted(sender.plan(false, attempt, request(Map.of(), 2)));
        assertNotEquals(first.body(), take().body());
        awaitCount("rejected", 1); assertEquals(4, count("http_attempt"));
    }

    @Test void secondaryHasSeparateCredentialsAndInvocationSpecificResult() throws Exception {
        var payload = Map.<String, Object>of("simulatorReceiptCodes", List.of("NONE"),
                "simulatorTcpReceiptCodes", List.of("RETRY_1S", "DELIVERED"));
        sender.accepted(sender.plan(true, attempt, request(payload, 1)));
        var first = take();
        assertEquals("/secondary", first.path()); assertEquals("Bearer " + SECONDARY, first.auth());
        assertEquals("FAILED", mapper.readTree(first.body()).get("outcome").asText());
        sender.accepted(sender.plan(true, attempt, request(payload, 2)));
        var second = take();
        assertEquals("DELIVERED", mapper.readTree(second.body()).get("code").asText());
        assertNotEquals(mapper.readTree(first.body()).get("receiptId"), mapper.readTree(second.body()).get("receiptId"));
    }

    @Test void missingAndDisabledDoNotSendAndInvalidScenarioHasNoEffect() throws Exception {
        sender.accepted(sender.plan(false, attempt, request(Map.of("simulatorReceiptCodes", List.of("NONE")), 1)));
        assertEquals(1, count("suppressed"));
        try (var disabled = new SimulatorReceiptSender(config(false, 10, 10), mapper, metrics)) {
            assertNull(disabled.plan(false, "legacy-key", request(Map.of(), null)));
            disabled.accepted(null);
        }
        var ledger = new SimulatorLedger(new SimulatorProperties(0, false, 10), metrics);
        var controller = new DeliveryProviderController(ledger, new SimulatorProperties(0, false, 10), sender);
        assertThrows(ResponseStatusException.class, () -> controller.receive(attempt,
                request(Map.of("simulatorReceiptCodes", List.of("ACCEPTED")), 1)));
        assertEquals(0.0, ledger.summary().get("calls"));
        assertNull(requests.poll(150, TimeUnit.MILLISECONDS));
    }

    @Test void pendingAndTrackingCapacityAreBoundedAndReleasedOnShutdown() throws Exception {
        sender.close();
        sender = new SimulatorReceiptSender(config(true, 2, 1), mapper, metrics);
        sender.accepted(sender.plan(false, attempt, request(Map.of("simulatorReceiptDelayMillis", 300000), 1)));
        assertThrows(ResponseStatusException.class, () -> sender.accepted(sender.plan(false, attempt, request(Map.of(), 2))));
        assertEquals(1, count("queue_full"));
        sender.accepted(sender.plan(false, attempt, request(Map.of("simulatorReceiptCodes", List.of("NONE")), 2)));
        assertThrows(ResponseStatusException.class, () -> sender.accepted(sender.plan(false, attempt, request(Map.of(), 3))));
        assertEquals(1, count("tracking_full"));
        sender.close(); awaitCount("cancelled", 1);
        assertNull(requests.poll());
    }

    @Test void callbackArrivesWhileProviderResponseIsStillHeld() throws Exception {
        var ledger = new SimulatorLedger(new SimulatorProperties(0, false, 10), metrics);
        var controller = new DeliveryProviderController(ledger, new SimulatorProperties(0, false, 10), sender);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var response = executor.submit(() -> controller.receive(attempt,
                    request(Map.of("simulatorReceiptDelayMillis", 0, "simulatorResponseDelayMillis", 1000), 1)));
            assertNotNull(take()); assertFalse(response.isDone());
            assertTrue(response.get(3, TimeUnit.SECONDS).getBody().accepted());
        }
    }

    @Test void lostTcpReplyStillEmitsSecondaryReceiptButRejectionDoesNot() throws Exception {
        var ledger = new SimulatorLedger(new SimulatorProperties(0, false, 10), metrics);
        try (var tcp = new TcpSimulatorServer(new TcpSimulatorProperties(true, "127.0.0.1", 0, 10, 2000), ledger, mapper, sender)) {
            tcp.start();
            try (var socket = new Socket("127.0.0.1", tcp.port())) {
                socket.setSoTimeout(2000);
                TcpFrames.write(socket.getOutputStream(), mapper.writeValueAsBytes(new TcpDeliveryRequest(delivery, attempt, 1L, "SMS",
                        Map.of("simulatorTcpMode", "close-after-effect"), Instant.now(), 1)));
                assertThrows(EOFException.class, () -> TcpFrames.read(socket.getInputStream()));
            }
            assertEquals("/secondary", take().path());
            try (var socket = new Socket("127.0.0.1", tcp.port())) {
                socket.setSoTimeout(2000);
                TcpFrames.write(socket.getOutputStream(), mapper.writeValueAsBytes(new TcpDeliveryRequest(delivery, attempt, 1L, "SMS",
                        Map.of("simulatorTcpResultCode", "REJECTED"), Instant.now(), 2)));
                assertFalse(mapper.readTree(TcpFrames.read(socket.getInputStream())).get("accepted").asBoolean());
            }
            assertNull(requests.poll(200, TimeUnit.MILLISECONDS));
        }
    }

    @Test void invalidCredentialsAndUrlAreRejectedWithoutLeakingSecret() {
        var good = config(true, 10, 10);
        assertFalse(good.toString().contains(PRIMARY));
        assertThrows(IllegalArgumentException.class, () -> new SimulatorReceiptProperties(true, URI.create("file:///tmp/test"),
                good.secondaryUrl(), PRIMARY, SECONDARY, 10, 10, 3, 2000, 20));
        assertThrows(IllegalArgumentException.class, () -> new SimulatorReceiptProperties(true, good.primaryUrl(),
                good.secondaryUrl(), PRIMARY, PRIMARY, 10, 10, 3, 2000, 20));
    }

    @Test void stalledResponseBodyHasBoundedDeadlineAndRetryBudget() throws Exception {
        var release = new CountDownLatch(1);
        var headersSent = new CountDownLatch(1);
        server.createContext("/stalled", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(202, 10);
            exchange.getResponseBody().write(1); exchange.getResponseBody().flush();
            headersSent.countDown();
            try { release.await(3, TimeUnit.SECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            finally { exchange.close(); }
        });
        sender.close();
        var config = config(true, 10, 10);
        sender = new SimulatorReceiptSender(new SimulatorReceiptProperties(true,
                config.primaryUrl().resolve("/stalled"), config.secondaryUrl(), PRIMARY, SECONDARY,
                10, 10, 2, 250, 20), mapper, metrics);
        try {
            sender.accepted(sender.plan(false, attempt, request(Map.of("simulatorReceiptDelayMillis", 0), 1)));
            assertTrue(headersSent.await(2, TimeUnit.SECONDS));
            awaitCount("exhausted", 1);
            assertEquals(2, count("http_attempt")); assertEquals(0, count("acknowledged"));
        } finally { release.countDown(); }
    }

    SimulatorReceiptProperties config(boolean enabled, int tracked, int pending) {
        String base = "http://127.0.0.1:" + server.getAddress().getPort();
        return new SimulatorReceiptProperties(enabled, URI.create(base + "/primary"), URI.create(base + "/secondary"),
                PRIMARY, SECONDARY, tracked, pending, 3, 2000, 20);
    }
    ProviderDispatchRequest request(Map<String, Object> payload, Integer invocation) {
        return new ProviderDispatchRequest(delivery, 1L, "SMS", payload, Instant.now(), invocation);
    }
    Captured take() throws Exception { var item = requests.poll(3, TimeUnit.SECONDS); assertNotNull(item); return item; }
    double count(String outcome) { var c = metrics.find("simulator.receipt.events").tag("outcome", outcome).counter(); return c == null ? 0 : c.count(); }
    void awaitCount(String outcome, int expected) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (count(outcome) < expected && System.nanoTime() < until) Thread.sleep(10);
        assertEquals(expected, count(outcome));
    }
    record Captured(String path, String auth, String body) { }
}

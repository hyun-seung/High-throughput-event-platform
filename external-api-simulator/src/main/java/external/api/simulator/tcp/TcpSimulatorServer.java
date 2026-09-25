package external.api.simulator.tcp;

import event.common.tcp.TcpDeliveryRequest;
import event.common.tcp.TcpDeliveryResponse;
import event.common.tcp.TcpFrames;
import external.api.simulator.delivery.dto.ProviderDispatchRequest;
import external.api.simulator.delivery.service.SimulatorLedger;
import external.api.simulator.receipt.SimulatorReceiptSender;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Test provider only. The ledger is process-local evidence, not durable vendor delivery storage. */
@Component
@EnableConfigurationProperties(TcpSimulatorProperties.class)
public class TcpSimulatorServer implements SmartLifecycle, AutoCloseable {
    private final TcpSimulatorProperties properties;
    private final SimulatorLedger ledger;
    private final JsonMapper mapper;
    private final SimulatorReceiptSender receipts;
    private final Set<Socket> active = ConcurrentHashMap.newKeySet();
    private final Semaphore capacity;
    private volatile boolean running;
    private ServerSocket server;
    private ExecutorService workers;
    private ScheduledExecutorService timeouts;

    public TcpSimulatorServer(TcpSimulatorProperties properties, SimulatorLedger ledger, JsonMapper mapper,
                              SimulatorReceiptSender receipts) {
        this.properties = properties;
        this.ledger = ledger;
        this.mapper = mapper;
        this.receipts = receipts;
        capacity = new Semaphore(properties.maxConnections());
    }

    @Override
    public synchronized void start() {
        if (!properties.enabled() || running) return;
        try {
            server = new ServerSocket();
            server.bind(new InetSocketAddress(properties.bindAddress(), properties.port()));
            workers = Executors.newVirtualThreadPerTaskExecutor();
            timeouts = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("tcp-simulator-deadline").factory());
            running = true;
            workers.submit(this::accept);
        } catch (IOException failure) {
            close();
            throw new IllegalStateException("Cannot bind TCP simulator", failure);
        }
    }

    private void accept() {
        while (running) {
            try {
                Socket socket = server.accept();
                if (!capacity.tryAcquire()) { socket.close(); continue; }
                active.add(socket);
                try {
                    workers.submit(() -> handle(socket));
                } catch (RuntimeException stopping) {
                    active.remove(socket);
                    capacity.release();
                    socket.close();
                }
            } catch (IOException failure) {
                if (running) stop();
                return;
            }
        }
    }

    private void handle(Socket socket) {
        java.util.concurrent.ScheduledFuture<?> timeout = null;
        try (socket) {
            while (running && !socket.isClosed()) {
                timeout = timeouts.schedule(() -> closeSocket(socket), properties.exchangeTimeoutMillis(), TimeUnit.MILLISECONDS);
                socket.setSoTimeout(properties.exchangeTimeoutMillis());
                var request = mapper.readValue(TcpFrames.read(socket.getInputStream()), TcpDeliveryRequest.class);
                if (request == null || request.attemptId() == null || request.payload() == null) return;
                Object mode = request.payload().getOrDefault("simulatorTcpMode", "normal");
                if (!Set.of("normal", "close-after-effect", "wrong-attempt").contains(mode)) return;
                var payload = new HashMap<>(request.payload());
                // First-provider failure scenarios must not accidentally reject the alternative provider.
                payload.remove("forceFail");
                payload.put("simulatorResultCode", payload.getOrDefault("simulatorTcpResultCode", "ACCEPTED"));
                var providerRequest = new ProviderDispatchRequest(request.deliveryId(),
                        request.tenantId(), request.deliveryType(), payload, request.occurredAt(), request.invocation());
                var receipt = receipts.plan(true, request.attemptId(), providerRequest);
                var received = ledger.receive("tcp:" + request.attemptId(), providerRequest);
                if (received.accepted()) receipts.accepted(receipt);
                if (mode.equals("close-after-effect")) return;
                SimulatorReceiptSender.delayResponse(receipt);
                var response = new TcpDeliveryResponse(received.deliveryId(), mode.equals("wrong-attempt") ? "unrelated" : request.attemptId(),
                        received.accepted(), received.processedAt(), received.accepted() ? "RECEIVED" : received.code());
                TcpFrames.write(socket.getOutputStream(), mapper.writeValueAsBytes(response));
                // If the deadline is already firing, leave this stream closed rather than reuse it.
                if (!timeout.cancel(false)) return;
                timeout = null;
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException ignored) {
            // A connection closed after acceptance may already have queued a receipt.
            // Never log payloads or raw parser exceptions.
        } finally {
            if (timeout != null) timeout.cancel(false);
            active.remove(socket);
            capacity.release();
        }
    }

    public int port() { return server.getLocalPort(); }
    @Override public boolean isRunning() { return running; }
    @Override public void stop() { close(); }

    @Override
    public synchronized void close() {
        running = false;
        if (server != null) try { server.close(); } catch (IOException ignored) { }
        active.forEach(TcpSimulatorServer::closeSocket);
        if (workers != null) workers.shutdownNow();
        if (timeouts != null) timeouts.shutdownNow();
    }

    private static void closeSocket(Socket socket) {
        try { socket.close(); } catch (IOException ignored) { }
    }
}

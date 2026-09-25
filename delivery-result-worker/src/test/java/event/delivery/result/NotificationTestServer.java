package event.delivery.result;

import com.sun.net.httpserver.HttpServer;
import event.common.notification.CustomerResultBatch;
import tools.jackson.databind.json.JsonMapper;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Actual local HTTP endpoint. Captures wire content before optionally losing its response. */
class NotificationTestServer implements AutoCloseable {
    static final String TOKEN = "notification-local-test-secret-123456";
    record Received(String body, String key, String authorization, CustomerResultBatch batch) {}
    final List<Received> requests = new CopyOnWriteArrayList<>();
    final Set<String> effects = ConcurrentHashMap.newKeySet();
    final AtomicInteger status = new AtomicInteger(204);
    final AtomicInteger redirects = new AtomicInteger();
    volatile long slowTenant = -1;
    volatile CountDownLatch slowStarted = new CountDownLatch(1), releaseSlow = new CountDownLatch(1);
    final HttpServer server;
    final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

    NotificationTestServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(workers);
        server.createContext("/results", exchange -> {
            try {
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                var batch = JsonMapper.builder().build().readValue(body, CustomerResultBatch.class);
                requests.add(new Received(body, exchange.getRequestHeaders().getFirst("Idempotency-Key"), exchange.getRequestHeaders().getFirst("Authorization"), batch));
                int code = status.get();
                if (code == 204 || code == 0) batch.results().forEach(r -> effects.add(r.eventId()));
                if (batch.tenantId() == slowTenant) {
                    slowStarted.countDown();
                    try { releaseSlow.await(10, TimeUnit.SECONDS); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }
                if (code == 0) return;
                exchange.getResponseHeaders().add("Location", url().resolve("/unexpected-redirect").toString());
                exchange.sendResponseHeaders(code, -1);
            } finally { exchange.close(); }
        });
        server.createContext("/unexpected-redirect", exchange -> {
            redirects.incrementAndGet(); exchange.sendResponseHeaders(204, -1); exchange.close();
        });
        server.start();
    }
    URI url() { return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/results"); }
    @Override public void close() { releaseSlow.countDown(); server.stop(0); workers.shutdownNow(); }
}

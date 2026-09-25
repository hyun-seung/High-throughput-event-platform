package event.delivery.result.notification;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class CustomerNotificationClient implements AutoCloseable {
    public record Result(boolean acknowledged, String error) {}
    private final Duration timeout;
    private final HttpClient client;

    public CustomerNotificationClient(Duration timeout) {
        this.timeout = timeout;
        client = HttpClient.newBuilder().connectTimeout(timeout).followRedirects(HttpClient.Redirect.NEVER)
                .version(HttpClient.Version.HTTP_1_1).build();
    }

    public Result send(NotificationRepository.Claim claim, NotificationProperties.Destination destination) {
        if (!claim.url().equals(destination.url().toString())) throw new IllegalArgumentException("Notification destination changed");
        var request = HttpRequest.newBuilder(URI.create(claim.url())).timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + destination.token())
                .header("Idempotency-Key", claim.batchId().toString())
                .POST(HttpRequest.BodyPublishers.ofString(claim.body())).build();
        // Complete on headers; do not read or buffer an arbitrary customer response body.
        var future = client.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream());
        try {
            var response = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            try (var ignored = response.body()) {
                return new Result(response.statusCode() == 204, "HTTP_" + response.statusCode());
            }
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            return new Result(false, "INTERRUPTED");
        } catch (java.util.concurrent.TimeoutException e) {
            future.cancel(true);
            return new Result(false, "TIMEOUT");
        } catch (java.util.concurrent.ExecutionException e) {
            future.cancel(true);
            return new Result(false, e.getCause() instanceof HttpTimeoutException ? "TIMEOUT" : "NETWORK_ERROR");
        } catch (Exception e) {
            future.cancel(true);
            return new Result(false, "NETWORK_ERROR");
        }
    }

    @Override public void close() { client.shutdownNow(); }
}

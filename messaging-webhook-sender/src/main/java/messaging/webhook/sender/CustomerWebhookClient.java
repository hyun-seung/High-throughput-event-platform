package messaging.webhook.sender;

import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Component
public class CustomerWebhookClient implements AutoCloseable {
    public record Result(boolean acknowledged, Integer httpStatus, String error) { }

    private final Duration timeout;
    private final HttpClient client;

    public CustomerWebhookClient(WebhookSenderProperties properties) {
        timeout = properties.httpTimeout();
        client = HttpClient.newBuilder().connectTimeout(timeout).followRedirects(HttpClient.Redirect.NEVER)
                .version(HttpClient.Version.HTTP_1_1).build();
    }

    public Result send(WebhookBatchRepository.Claim claim, WebhookSenderProperties.Destination destination) {
        if (!claim.url().equals(destination.url().toString())) throw new IllegalArgumentException("Webhook destination changed");
        var request = HttpRequest.newBuilder(URI.create(claim.url())).timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + destination.token())
                .header("Idempotency-Key", claim.batchId().toString())
                .POST(HttpRequest.BodyPublishers.ofString(claim.body())).build();
        var future = client.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream());
        try {
            var response = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            try (var ignored = response.body()) {
                int status = response.statusCode();
                return new Result(status == 204, status, status == 204 ? null : "HTTP_" + status);
            }
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            return new Result(false, null, "INTERRUPTED");
        } catch (java.util.concurrent.TimeoutException e) {
            future.cancel(true);
            return new Result(false, null, "TIMEOUT");
        } catch (java.util.concurrent.ExecutionException e) {
            future.cancel(true);
            return new Result(false, null, e.getCause() instanceof HttpTimeoutException ? "TIMEOUT" : "NETWORK_ERROR");
        } catch (Exception e) {
            future.cancel(true);
            return new Result(false, null, "NETWORK_ERROR");
        }
    }

    @Override public void close() { client.shutdownNow(); }
}

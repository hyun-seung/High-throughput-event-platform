package messaging.webhook.sender;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

@ConfigurationProperties("messaging.webhook.sender")
public record WebhookSenderProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("100") long pollMs,
        @DefaultValue("4") int concurrency,
        @DefaultValue("100ms") Duration batchWindow,
        @DefaultValue("262144") int maxBatchBytes,
        @DefaultValue("3s") Duration httpTimeout,
        @DefaultValue("30s") Duration lease,
        @DefaultValue("1s") Duration retryDelay,
        @DefaultValue("60s") Duration maxRetryDelay,
        Map<Long, Destination> customers) {
    public WebhookSenderProperties {
        customers = customers == null ? Map.of() : Map.copyOf(customers);
        if (pollMs < 10 || concurrency < 1 || concurrency > 64 || batchWindow == null || batchWindow.isNegative()
                || maxBatchBytes < 16384 || maxBatchBytes > 1048576 || httpTimeout == null || httpTimeout.isNegative()
                || lease == null || lease.compareTo(httpTimeout.plusSeconds(5)) < 0
                || retryDelay == null || retryDelay.isNegative() || maxRetryDelay == null
                || maxRetryDelay.compareTo(retryDelay) < 0 || customers.keySet().stream().anyMatch(id -> id <= 0)) {
            throw new IllegalArgumentException("Invalid customer webhook settings");
        }
    }

    public record Destination(URI url) {
        public Destination {
            if (url == null || url.getHost() == null || url.getUserInfo() != null || url.getFragment() != null
                    || !("https".equals(url.getScheme()) || "http".equals(url.getScheme())
                    && Set.of("localhost", "127.0.0.1", "[::1]").contains(url.getHost()))) {
                throw new IllegalArgumentException("Customer webhook requires HTTPS (or loopback HTTP)");
            }
        }
    }

    public Duration retryAfter(int attempt) {
        long multiplier = 1L << Math.min(Math.max(attempt - 1, 0), 20);
        long cap = maxRetryDelay.toMillis();
        long base = retryDelay.toMillis();
        return Duration.ofMillis(base > cap / multiplier ? cap : Math.min(cap, base * multiplier));
    }
}

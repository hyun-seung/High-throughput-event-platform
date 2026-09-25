package event.delivery.result.notification;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

@ConfigurationProperties("notification")
public record NotificationProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("4") int concurrency,
        @DefaultValue("100") long pollMs,
        @DefaultValue("3s") Duration httpTimeout,
        @DefaultValue("30s") Duration lease,
        @DefaultValue("1s") Duration retryDelay,
        @DefaultValue("60s") Duration maxRetryDelay,
        @DefaultValue("262144") int maxBatchBytes,
        Map<Long, Destination> customers) {
    public NotificationProperties {
        customers = customers == null ? Map.of() : Map.copyOf(customers);
        if (concurrency < 1 || concurrency > 64 || pollMs < 10 || pollMs > 1000
                || httpTimeout == null || httpTimeout.toMillis() < 50 || httpTimeout.compareTo(Duration.ofSeconds(30)) > 0
                || lease == null || lease.compareTo(httpTimeout.plusSeconds(5)) < 0
                || retryDelay == null || retryDelay.toMillis() < 1 || maxRetryDelay == null || maxRetryDelay.compareTo(retryDelay) < 0
                || maxBatchBytes < 16_384 || maxBatchBytes > 1_048_576
                || customers.keySet().stream().anyMatch(id -> id <= 0)
                || (enabled && customers.isEmpty())) throw new IllegalArgumentException("Invalid notification settings");
    }

    public record Destination(URI url, String token) {
        public Destination {
            if (url == null || url.getHost() == null || url.getUserInfo() != null || url.getFragment() != null
                    || !("https".equals(url.getScheme()) || ("http".equals(url.getScheme()) && Set.of("localhost", "127.0.0.1", "[::1]").contains(url.getHost())))
                    || token == null || !token.matches("[!-~]{32,256}")) {
                throw new IllegalArgumentException("Notification destination needs HTTPS (or loopback HTTP) and a 32-256 character token");
            }
        }
        @Override public String toString() { return "Destination[url=" + url + ", token=<redacted>]"; }
    }

    public Duration retryAfter(int attempt) {
        long multiplier = 1L << Math.min(Math.max(attempt - 1, 0), 20);
        long base = retryDelay.toMillis(), cap = maxRetryDelay.toMillis();
        return Duration.ofMillis(base > cap / multiplier ? cap : Math.min(cap, base * multiplier));
    }
}

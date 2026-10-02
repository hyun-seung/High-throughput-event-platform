package event.http.sender;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;

import java.time.Duration;

@ConfigurationProperties(prefix = "event.http.sender")
public record HttpSenderProperties(String provider, String baseUrl, Duration connectTimeout,
                                   Duration readTimeout, Duration acquireTimeout, int maxConnections,
                                   Duration primaryTtl, Duration leaseDuration) {
    @ConstructorBinding
    public HttpSenderProperties {
        provider = provider == null || provider.isBlank() ? "mock-provider" : provider;
        baseUrl = baseUrl == null || baseUrl.isBlank() ? "http://localhost:8090" : baseUrl;
        connectTimeout = connectTimeout == null ? Duration.ofSeconds(2) : connectTimeout;
        readTimeout = readTimeout == null ? Duration.ofSeconds(5) : readTimeout;
        acquireTimeout = acquireTimeout == null ? Duration.ofSeconds(2) : acquireTimeout;
        maxConnections = maxConnections == 0 ? 64 : maxConnections;
        primaryTtl = primaryTtl == null ? Duration.ofHours(3) : primaryTtl;
        leaseDuration = leaseDuration == null ? Duration.ofSeconds(30) : leaseDuration;
        if (connectTimeout.toMillis() < 1 || readTimeout.toMillis() < 1 || acquireTimeout.toMillis() < 1
                || primaryTtl.toMillis() < 1 || leaseDuration.toMillis() < 1 || maxConnections < 1) {
            throw new IllegalArgumentException("HTTP sender timeouts, TTL and pool size must be positive");
        }
    }
}

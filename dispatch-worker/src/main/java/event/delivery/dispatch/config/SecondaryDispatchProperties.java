package event.delivery.dispatch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;

@ConfigurationProperties("dispatch.secondary")
public record SecondaryDispatchProperties(String provider, Duration ttl) {
    public SecondaryDispatchProperties {
        provider = provider == null || provider.isBlank() ? "tcp-provider" : provider;
        ttl = ttl == null ? Duration.ofHours(4) : ttl;
        if (ttl.toMillis() < 1) throw new IllegalArgumentException("Secondary TTL must be at least 1ms");
    }
}

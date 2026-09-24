package event.delivery.dispatch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "dispatch")
public record DispatchProperties(
        String provider,
        Duration leaseDuration,
        Duration primaryTtl,
        Duration noResponseRetryDelay
) {

    public DispatchProperties(String provider, Duration leaseDuration) {
        this(provider, leaseDuration, null, null);
    }

    @org.springframework.boot.context.properties.bind.ConstructorBinding
    public DispatchProperties {
        provider = provider == null || provider.isBlank() ? "mock-provider" : provider;
        leaseDuration = leaseDuration == null ? Duration.ofSeconds(30) : leaseDuration;
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("dispatch.lease-duration must be positive");
        }
        primaryTtl = primaryTtl == null ? Duration.ofHours(3) : primaryTtl;
        noResponseRetryDelay = noResponseRetryDelay == null ? Duration.ofSeconds(1) : noResponseRetryDelay;
        if (primaryTtl.toMillis() < 1 || noResponseRetryDelay.toMillis() < 1) {
            throw new IllegalArgumentException("dispatch primary TTL and retry delay must be at least 1ms");
        }
    }
}

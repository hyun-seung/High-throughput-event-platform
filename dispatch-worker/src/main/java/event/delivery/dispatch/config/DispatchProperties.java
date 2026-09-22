package event.delivery.dispatch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "dispatch")
public record DispatchProperties(
        String provider,
        Duration leaseDuration
) {

    public DispatchProperties {
        provider = provider == null || provider.isBlank() ? "mock-provider" : provider;
        leaseDuration = leaseDuration == null ? Duration.ofSeconds(30) : leaseDuration;
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("dispatch.lease-duration must be positive");
        }
    }
}

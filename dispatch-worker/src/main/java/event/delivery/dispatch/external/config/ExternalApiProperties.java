package event.delivery.dispatch.external.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import java.time.Duration;

@ConfigurationProperties(prefix = "external-api")
public record ExternalApiProperties(String baseUrl, Duration connectTimeout, Duration readTimeout,
                                    int maxConnections, int maxConnectionsPerRoute, Duration acquireTimeout,
                                    Duration maxIdleTime) {
    public ExternalApiProperties(String baseUrl, Duration connectTimeout, Duration readTimeout) {
        this(baseUrl, connectTimeout, readTimeout, 64, 64, Duration.ofSeconds(2), Duration.ofSeconds(30));
    }

    @ConstructorBinding
    public ExternalApiProperties {
        connectTimeout = connectTimeout == null ? Duration.ofSeconds(2) : connectTimeout;
        readTimeout = readTimeout == null ? Duration.ofSeconds(5) : readTimeout;
        acquireTimeout = acquireTimeout == null ? Duration.ofSeconds(2) : acquireTimeout;
        maxIdleTime = maxIdleTime == null ? Duration.ofSeconds(30) : maxIdleTime;
        maxConnections = maxConnections == 0 ? 64 : maxConnections;
        maxConnectionsPerRoute = maxConnectionsPerRoute == 0 ? 64 : maxConnectionsPerRoute;
        if (maxConnections < 1 || maxConnectionsPerRoute < 1 || maxConnectionsPerRoute > maxConnections
                || connectTimeout.toMillis() < 1 || readTimeout.toMillis() < 1
                || acquireTimeout.toMillis() < 1 || maxIdleTime.toMillis() < 1) {
            throw new IllegalArgumentException("HTTP pool limits and timeouts must be positive; per-route limit must fit total");
        }
    }
}

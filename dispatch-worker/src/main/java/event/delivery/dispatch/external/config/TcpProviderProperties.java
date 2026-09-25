package event.delivery.dispatch.external.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import java.time.Duration;

@ConfigurationProperties("external-tcp")
public record TcpProviderProperties(String host, int port, Duration connectTimeout, Duration exchangeTimeout,
                                    int maxConnections, int maxPendingAcquires, Duration acquireTimeout, int ioThreads) {
    public TcpProviderProperties(String host, int port, Duration connectTimeout, Duration exchangeTimeout) {
        this(host, port, connectTimeout, exchangeTimeout, 32, 64, Duration.ofSeconds(2), 2);
    }

    @ConstructorBinding
    public TcpProviderProperties {
        host = host == null || host.isBlank() ? "localhost" : host;
        port = port == 0 ? 8093 : port;
        connectTimeout = connectTimeout == null ? Duration.ofSeconds(2) : connectTimeout;
        exchangeTimeout = exchangeTimeout == null ? Duration.ofSeconds(5) : exchangeTimeout;
        acquireTimeout = acquireTimeout == null ? Duration.ofSeconds(2) : acquireTimeout;
        maxConnections = maxConnections == 0 ? 32 : maxConnections;
        maxPendingAcquires = maxPendingAcquires == 0 ? 64 : maxPendingAcquires;
        ioThreads = ioThreads == 0 ? 2 : ioThreads;
        if (port < 1 || port > 65535 || maxConnections < 1 || maxPendingAcquires < 1 || ioThreads < 1
                || !valid(connectTimeout) || !valid(exchangeTimeout) || !valid(acquireTimeout)) {
            throw new IllegalArgumentException("Invalid TCP capacity, port or timeout (1..60000 ms)");
        }
    }

    private static boolean valid(Duration timeout) {
        return timeout.toMillis() >= 1 && timeout.toMillis() <= 60000;
    }
}

package event.delivery.dispatch.external.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;

@ConfigurationProperties("external-tcp")
public record TcpProviderProperties(String host, int port, Duration connectTimeout, Duration exchangeTimeout) {
    public TcpProviderProperties {
        host = host == null || host.isBlank() ? "localhost" : host;
        port = port == 0 ? 8093 : port;
        connectTimeout = connectTimeout == null ? Duration.ofSeconds(2) : connectTimeout;
        exchangeTimeout = exchangeTimeout == null ? Duration.ofSeconds(5) : exchangeTimeout;
        if (port < 1 || port > 65535 || connectTimeout.toMillis() < 1 || connectTimeout.toMillis() > 60000
                || exchangeTimeout.toMillis() < 1 || exchangeTimeout.toMillis() > 60000) {
            throw new IllegalArgumentException("Invalid TCP port or timeout (1..60000 ms)");
        }
    }
}

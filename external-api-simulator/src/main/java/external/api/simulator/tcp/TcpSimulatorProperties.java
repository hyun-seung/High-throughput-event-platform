package external.api.simulator.tcp;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("simulator.tcp")
public record TcpSimulatorProperties(@DefaultValue("true") boolean enabled,
                                     @DefaultValue("127.0.0.1") String bindAddress,
                                     @DefaultValue("8093") int port,
                                     @DefaultValue("128") int maxConnections,
                                     @DefaultValue("5000") int exchangeTimeoutMillis) {
    public TcpSimulatorProperties {
        if (port < 0 || port > 65535 || maxConnections < 1 || maxConnections > 10000
                || exchangeTimeoutMillis < 1 || exchangeTimeoutMillis > 60000) {
            throw new IllegalArgumentException("Invalid TCP simulator port, concurrency or timeout");
        }
    }
}

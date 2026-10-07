package messaging.tcp.sender;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

@ConfigurationProperties("messaging.tcp.sender")
public record TcpSenderProperties(@DefaultValue("true") boolean enabled, String host,
                                  @DefaultValue("18091") int port,
                                  @DefaultValue("2s") Duration connectTimeout,
                                  @DefaultValue("5s") Duration responseTimeout,
                                  @DefaultValue("30s") Duration claimTtl,
                                  @DefaultValue("30s") Duration recoveryGrace) {
    public TcpSenderProperties {
        if (enabled && (host == null || host.isBlank()) || port < 1 || port > 65535
                || connectTimeout == null || connectTimeout.isNegative() || connectTimeout.isZero()
                || responseTimeout == null || responseTimeout.isNegative() || responseTimeout.isZero()
                || claimTtl == null || claimTtl.isNegative() || claimTtl.isZero()
                || recoveryGrace == null || recoveryGrace.isNegative() || recoveryGrace.isZero()) {
            throw new IllegalArgumentException("Invalid TCP sender settings");
        }
    }
}

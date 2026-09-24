package external.api.simulator.delivery.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("simulator")
public record SimulatorProperties(@DefaultValue("0") long responseDelayMillis,
                                  @DefaultValue("false") boolean deduplicate,
                                  @DefaultValue("200000") int maxTrackedKeys) {
    public SimulatorProperties {
        if (responseDelayMillis < 0 || responseDelayMillis > 60000) {
            throw new IllegalArgumentException("simulator.response-delay-millis must be between 0 and 60000");
        }
        if (maxTrackedKeys < 1 || maxTrackedKeys > 1000000) {
            throw new IllegalArgumentException("simulator.max-tracked-keys must be between 1 and 1000000");
        }
    }
}

package event.common.dynamodb.config;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "delivery.dynamodb")
public class DynamoDbProperties {

    private String endpoint;
    private String region = "ap-northeast-2";
    private boolean initializeTables;
    private int maxConnections = 64;
    private Duration acquireTimeout = Duration.ofSeconds(2);
    private Duration connectTimeout = Duration.ofSeconds(2);
    private Duration socketTimeout = Duration.ofSeconds(5);
    private Duration maxIdleTime = Duration.ofSeconds(30);

    public void validateTransport() {
        if (maxConnections < 1 || !positive(acquireTimeout) || !positive(connectTimeout)
                || !positive(socketTimeout) || !positive(maxIdleTime)) {
            throw new IllegalArgumentException("DynamoDB connection limit and transport timeouts must be positive");
        }
    }

    private static boolean positive(Duration duration) {
        return duration != null && duration.toMillis() >= 1;
    }
}

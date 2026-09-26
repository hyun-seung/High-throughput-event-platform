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
    private Duration apiCallTimeout = Duration.ofSeconds(10);
    private Duration apiCallAttemptTimeout = Duration.ofSeconds(5);
    private int maxAttempts = 3;

    public void validateTransport() {
        if (maxConnections < 1 || !positive(acquireTimeout) || !positive(connectTimeout)
                || !positive(socketTimeout) || !positive(maxIdleTime)
                || !positive(apiCallTimeout) || !positive(apiCallAttemptTimeout)
                || apiCallAttemptTimeout.compareTo(apiCallTimeout) > 0
                || connectTimeout.compareTo(apiCallAttemptTimeout) > 0
                || acquireTimeout.compareTo(apiCallAttemptTimeout) > 0
                || socketTimeout.compareTo(apiCallAttemptTimeout) > 0
                || maxAttempts < 1 || maxAttempts > 10) {
            throw new IllegalArgumentException("DynamoDB requires positive limits, transport <= attempt <= call timeout, and 1..10 total attempts");
        }
    }

    private static boolean positive(Duration duration) {
        return duration != null && duration.toMillis() >= 1;
    }
}

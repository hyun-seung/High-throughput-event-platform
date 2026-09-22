package event.common.dynamodb.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "event.dynamodb")
public class DynamoDbProperties {

    private String endpoint;
    private String region = "ap-northeast-2";
    private boolean initializeTables;
}

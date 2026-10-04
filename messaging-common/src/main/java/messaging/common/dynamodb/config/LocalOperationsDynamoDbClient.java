package messaging.common.dynamodb.config;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.net.URI;
import java.time.Duration;

/** Bounded DynamoDB client for local operations CLIs that do not start Spring. */
public final class LocalOperationsDynamoDbClient {
    private LocalOperationsDynamoDbClient() { }

    public static DynamoDbClient create(URI endpoint) {
        return DynamoDbClient.builder().endpointOverride(endpoint).region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .httpClientBuilder(Apache5HttpClient.builder().maxConnections(4)
                        .connectionTimeout(Duration.ofSeconds(2))
                        .connectionAcquisitionTimeout(Duration.ofSeconds(2))
                        .socketTimeout(Duration.ofSeconds(5)))
                .overrideConfiguration(config -> config.apiCallTimeout(Duration.ofSeconds(10))
                        .apiCallAttemptTimeout(Duration.ofSeconds(5))
                        .retryStrategy(StandardRetryStrategy.builder().maxAttempts(3).build()))
                .build();
    }
}

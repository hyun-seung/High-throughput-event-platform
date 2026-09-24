package event.common.dynamodb.config;

import event.common.metrics.DynamoDbMetricsInterceptor;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.net.URI;

@AutoConfiguration
@ConditionalOnProperty(prefix = "delivery.dynamodb", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(DynamoDbProperties.class)
public class DynamoDbAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DynamoDbClient dynamoDbClient(DynamoDbProperties properties, ObjectProvider<MeterRegistry> registries) {
        DynamoDbClientBuilder builder = DynamoDbClient.builder()
                .region(Region.of(properties.getRegion()))
                .httpClientBuilder(UrlConnectionHttpClient.builder());
        MeterRegistry registry = registries.getIfAvailable();
        if (registry != null) {
            builder.overrideConfiguration(config -> config.addExecutionInterceptor(new DynamoDbMetricsInterceptor(registry)));
        }

        if (properties.getEndpoint() != null && !properties.getEndpoint().isBlank()) {
            builder.endpointOverride(URI.create(properties.getEndpoint()))
                    .credentialsProvider(
                            StaticCredentialsProvider.create(
                                    AwsBasicCredentials.create("dummy", "dummy")
                            )
                    );
        }

        return builder.build();
    }

    @Bean
    @ConditionalOnProperty(
            prefix = "delivery.dynamodb",
            name = "initialize-tables",
            havingValue = "true"
    )
    public DynamoDbTableInitializer dynamoDbTableInitializer(DynamoDbClient dynamoDbClient) {
        return new DynamoDbTableInitializer(dynamoDbClient);
    }
}

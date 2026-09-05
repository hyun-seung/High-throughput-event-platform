package event.processing.worker.external.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
@EnableConfigurationProperties(ExternalApiProperties.class)
public class ExternalApiClientConfig {

    @Bean
    public RestClient externalApiRestClient(
            RestClient.Builder builder,
            ExternalApiProperties properties
    ) {
        return builder
                .baseUrl(properties.baseUrl())
                .build();
    }
}
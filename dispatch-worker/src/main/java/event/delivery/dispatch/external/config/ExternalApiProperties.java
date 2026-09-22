package event.delivery.dispatch.external.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "external-api")
public record ExternalApiProperties(
        String baseUrl,
        Duration connectTimeout,
        Duration readTimeout
) {

    public ExternalApiProperties {
        connectTimeout = connectTimeout == null ? Duration.ofSeconds(2) : connectTimeout;
        readTimeout = readTimeout == null ? Duration.ofSeconds(5) : readTimeout;
    }
}

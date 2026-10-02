package event.http.sender;

import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.pool.PoolConcurrencyPolicy;
import org.apache.hc.core5.util.Timeout;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.web.client.RestClient;

import java.time.Clock;

@Configuration
@EnableConfigurationProperties(HttpSenderProperties.class)
public class HttpSenderConfiguration {
    @Bean Clock httpSenderClock() { return Clock.systemUTC(); }

    @Bean
    DefaultErrorHandler httpRequestErrorHandler() {
        // Retain the Kafka offset until STEP storage and outcome publication are confirmed.
        return new DefaultErrorHandler(new FixedBackOff(1000, FixedBackOff.UNLIMITED_ATTEMPTS));
    }

    @Bean(destroyMethod = "close")
    CloseableHttpClient providerHttpClient(HttpSenderProperties properties) {
        var manager = PoolingHttpClientConnectionManagerBuilder.create()
                .setPoolConcurrencyPolicy(PoolConcurrencyPolicy.STRICT)
                .setMaxConnTotal(properties.maxConnections())
                .setMaxConnPerRoute(properties.maxConnections())
                .setDefaultConnectionConfig(ConnectionConfig.custom()
                        .setConnectTimeout(Timeout.ofMilliseconds(properties.connectTimeout().toMillis()))
                        .setSocketTimeout(Timeout.ofMilliseconds(properties.readTimeout().toMillis())).build()).build();
        return HttpClients.custom().setConnectionManager(manager)
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofMilliseconds(properties.acquireTimeout().toMillis()))
                        .setResponseTimeout(Timeout.ofMilliseconds(properties.readTimeout().toMillis())).build())
                .disableAutomaticRetries().disableRedirectHandling().disableCookieManagement().build();
    }

    @Bean
    RestClient providerRestClient(RestClient.Builder builder, HttpSenderProperties properties,
                                  CloseableHttpClient providerHttpClient) {
        return builder.baseUrl(properties.baseUrl())
                .requestFactory(new HttpComponentsClientHttpRequestFactory(providerHttpClient)).build();
    }
}

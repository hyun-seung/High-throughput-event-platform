package messaging.carrier.sender;

import messaging.common.messages.CarrierHttpResult;
import messaging.common.messages.MessageTopics;
import messaging.common.messages.RedisSendAttemptGuard;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.util.Timeout;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.web.client.RestClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(CarrierSenderProperties.class)
public class CarrierSenderConfiguration {
    @Bean Clock carrierSenderClock() { return Clock.systemUTC(); }

    @Bean RedisSendAttemptGuard carrierSendAttemptGuard(StringRedisTemplate redis) {
        return new RedisSendAttemptGuard(redis);
    }

    @Bean CarrierHttpAttemptStore carrierHttpAttemptStore(DynamoDbClient db, JsonMapper mapper) {
        return new CarrierHttpAttemptStore(db, mapper);
    }

    @Bean CarrierSendGate carrierSendGate(CarrierSenderProperties properties, CarrierHttpAttemptStore attempts,
                                          RedisSendAttemptGuard redis, Clock clock) {
        return new CarrierSendGate(properties.carrier(), attempts, redis, clock, properties.claimTtl());
    }

    @Bean CarrierErrorNormalizer carrierErrorNormalizer(CarrierSenderProperties properties) {
        return new CarrierErrorNormalizer(properties.notOurCarrierCodeSet(), properties.tpsExceededCodeSet());
    }

    @Bean CarrierHttpSendService carrierHttpSendService(CarrierSendGate gate, CarrierHttpAttemptStore attempts,
                                                        CarrierProviderClient provider, CarrierErrorNormalizer normalizer,
                                                        KafkaTemplate<String, CarrierHttpResult> kafka,
                                                        Clock clock, CarrierSenderProperties properties) {
        return new CarrierHttpSendService(gate, attempts, provider, normalizer, kafka, clock,
                properties.recoveryGrace());
    }

    @Bean(destroyMethod = "close")
    CloseableHttpClient carrierHttpClient(CarrierSenderProperties properties) {
        var manager = PoolingHttpClientConnectionManagerBuilder.create().setMaxConnTotal(64)
                .setMaxConnPerRoute(64)
                .setDefaultConnectionConfig(ConnectionConfig.custom()
                        .setConnectTimeout(Timeout.ofMilliseconds(properties.connectTimeout().toMillis()))
                        .setSocketTimeout(Timeout.ofMilliseconds(properties.responseTimeout().toMillis())).build())
                .build();
        return HttpClients.custom().setConnectionManager(manager)
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofSeconds(2))
                        .setResponseTimeout(Timeout.ofMilliseconds(properties.responseTimeout().toMillis())).build())
                .disableAutomaticRetries().disableRedirectHandling().disableCookieManagement().build();
    }

    @Bean CarrierProviderClient carrierProviderClient(RestClient.Builder builder, JsonMapper mapper,
                                                     CarrierSenderProperties properties,
                                                     CloseableHttpClient carrierHttpClient) {
        RestClient rest = builder.baseUrl(properties.baseUrl())
                .requestFactory(new HttpComponentsClientHttpRequestFactory(carrierHttpClient)).build();
        return new CarrierProviderClient(rest, mapper, properties.requestPath());
    }

    @Bean DefaultErrorHandler carrierHttpErrorHandler() {
        var handler = new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setClassifications(Map.of(Exception.class, true), true);
        return handler;
    }

    @Bean NewTopic carrierResultTopic(@Value("${messaging.carrier.sender.result-partitions:3}") int partitions,
                                      @Value("${messaging.carrier.sender.result-replicas:1}") int replicas) {
        if (partitions < 1 || replicas < 1) throw new IllegalArgumentException("Invalid result topic settings");
        return TopicBuilder.name(MessageTopics.MSG_RESULT).partitions(partitions).replicas(replicas)
                .config("min.insync.replicas", "1").config("cleanup.policy", "delete")
                .config("retention.ms", "604800000").build();
    }

    @Bean NewTopic carrierCommandTopic(CarrierSenderProperties properties,
                                        @Value("${messaging.carrier.sender.command-partitions:3}") int partitions,
                                        @Value("${messaging.carrier.sender.command-replicas:1}") int replicas) {
        if (partitions < 1 || replicas < 1) throw new IllegalArgumentException("Invalid command topic settings");
        return TopicBuilder.name(properties.topic()).partitions(partitions).replicas(replicas)
                .config("cleanup.policy", "delete").config("retention.ms", "604800000").build();
    }
}

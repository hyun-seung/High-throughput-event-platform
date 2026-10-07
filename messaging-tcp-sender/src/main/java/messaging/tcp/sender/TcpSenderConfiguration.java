package messaging.tcp.sender;

import messaging.common.messages.RedisSendAttemptGuard;
import messaging.common.messages.MessageTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Clock;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(TcpSenderProperties.class)
public class TcpSenderConfiguration {
    @Bean Clock tcpSenderClock() { return Clock.systemUTC(); }

    @Bean RedisSendAttemptGuard tcpSendAttemptGuard(StringRedisTemplate redis) {
        return new RedisSendAttemptGuard(redis);
    }

    @Bean DefaultErrorHandler tcpSenderErrorHandler() {
        var handler = new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setClassifications(Map.of(Exception.class, true), true);
        return handler;
    }

    @Bean
    @ConditionalOnProperty(prefix = "messaging.tcp.sender", name = "enabled", havingValue = "true")
    NewTopic tcpResultTopic(@Value("${messaging.tcp.sender.result-partitions:3}") int partitions,
                            @Value("${messaging.tcp.sender.result-replicas:1}") int replicas) {
        if (partitions < 1 || replicas < 1) throw new IllegalArgumentException("Invalid TCP result topic settings");
        return TopicBuilder.name(MessageTopics.MSG_RESULT).partitions(partitions).replicas(replicas)
                .config("min.insync.replicas", "1").config("cleanup.policy", "delete")
                .config("retention.ms", "604800000").build();
    }
}

package messaging.pre.send;

import messaging.common.messages.PrimaryExpiryIndex;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.Map;

import static messaging.common.messages.MessageTopics.*;

@Configuration
public class PreSendConfiguration {
    @Bean Clock preSendClock() { return Clock.systemUTC(); }

    @Bean PreSendReferenceReader preSendReferenceReader(StringRedisTemplate redis, JdbcTemplate jdbc, JsonMapper mapper) {
        return new PreSendReferenceReader(redis, jdbc, mapper);
    }

    @Bean InitialCarrierStore initialCarrierStore(DynamoDbClient db, JsonMapper mapper) {
        return new InitialCarrierStore(db, mapper);
    }

    @Bean PreSendPreparation preSendPreparation(PreSendReferenceReader references, InitialCarrierStore carriers,
                                               Clock clock) {
        return new PreSendPreparation(references, carriers, clock, PrimaryExpiryIndex.TTL);
    }

    @Bean PreSendDecisionStore preSendDecisionStore(DynamoDbClient db, JsonMapper mapper,
                                                     PreSendPreparation preparation, Clock clock) {
        return new PreSendDecisionStore(db, mapper, preparation, clock);
    }

    @Bean DefaultErrorHandler preSendErrorHandler() {
        var handler = new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        // Never advance the admission offset merely because a malformed record or a dependency failed.
        handler.setClassifications(Map.of(Exception.class, true), true);
        return handler;
    }

    @Bean NewTopic receivedTopic(@Value("${messaging.pre-send.partitions:3}") int partitions,
                                @Value("${messaging.pre-send.replicas:1}") int replicas) {
        return topic(RECEIVED, partitions, replicas);
    }

    @Bean NewTopic preSendResultTopic(@Value("${messaging.pre-send.partitions:3}") int partitions,
                                     @Value("${messaging.pre-send.replicas:1}") int replicas) {
        return topic(MSG_RESULT, partitions, replicas);
    }

    @Bean NewTopic sktSendTopic(@Value("${messaging.pre-send.partitions:3}") int partitions,
                               @Value("${messaging.pre-send.replicas:1}") int replicas) {
        return topic(SKT_HTTP_SEND, partitions, replicas);
    }

    @Bean NewTopic ktSendTopic(@Value("${messaging.pre-send.partitions:3}") int partitions,
                              @Value("${messaging.pre-send.replicas:1}") int replicas) {
        return topic(KT_HTTP_SEND, partitions, replicas);
    }

    @Bean NewTopic lguSendTopic(@Value("${messaging.pre-send.partitions:3}") int partitions,
                               @Value("${messaging.pre-send.replicas:1}") int replicas) {
        return topic(LGU_HTTP_SEND, partitions, replicas);
    }

    private static NewTopic topic(String name, int partitions, int replicas) {
        if (partitions < 1 || replicas < 1) throw new IllegalArgumentException("Invalid HTTP send topic settings");
        return TopicBuilder.name(name).partitions(partitions).replicas(replicas)
                .config("cleanup.policy", "delete").config("retention.ms", "604800000").build();
    }
}

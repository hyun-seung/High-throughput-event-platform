package messaging.result;

import messaging.common.messages.HttpSendCommand;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.Map;

@Configuration
public class ResultManagerConfiguration {
    @Bean Clock resultManagerClock() { return Clock.systemUTC(); }

    @Bean MessageResultRecordCodec messageResultRecordCodec(JsonMapper mapper) {
        return new MessageResultRecordCodec(mapper);
    }

    @Bean MessageResultInboxStore messageResultInboxStore(DynamoDbClient db, JsonMapper mapper, Clock clock) {
        return new MessageResultInboxStore(db, mapper, clock);
    }

    @Bean WebhookPrimaryDecisionService webhookPrimaryDecisionService(DynamoDbClient db, JsonMapper mapper,
                                                                       FollowupHttpCommandStore commands, Clock clock) {
        return new WebhookPrimaryDecisionService(db, mapper, commands, clock);
    }

    @Bean DefaultErrorHandler resultErrorHandler() {
        var handler = new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setClassifications(Map.of(Exception.class, true), true);
        return handler;
    }

    @Bean FollowupHttpCommandStore followupHttpCommandStore(DynamoDbClient db, JsonMapper mapper) {
        return new FollowupHttpCommandStore(db, mapper);
    }

    @Bean HttpFailureFollowupService httpFailureFollowupService(DynamoDbClient db, JsonMapper mapper,
                                                                FollowupHttpCommandStore commands, Clock clock) {
        return new HttpFailureFollowupService(db, mapper, commands, clock);
    }

    @Bean FollowupHttpDispatcher followupHttpDispatcher(DynamoDbClient db, JsonMapper mapper,
                                                        KafkaTemplate<String, HttpSendCommand> kafka, Clock clock,
                                                        @Value("${messaging.result.followup.page-size:100}") int pageSize) {
        return new FollowupHttpDispatcher(db, mapper, kafka, clock, pageSize);
    }
}

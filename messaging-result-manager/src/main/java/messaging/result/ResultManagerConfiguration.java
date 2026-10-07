package messaging.result;

import messaging.common.messages.HttpSendCommand;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;

@Configuration
public class ResultManagerConfiguration {
    @Bean Clock resultManagerClock() { return Clock.systemUTC(); }

    @Bean FollowupHttpCommandStore followupHttpCommandStore(DynamoDbClient db, JsonMapper mapper) {
        return new FollowupHttpCommandStore(db, mapper);
    }

    @Bean FollowupHttpDispatcher followupHttpDispatcher(DynamoDbClient db, JsonMapper mapper,
                                                        KafkaTemplate<String, HttpSendCommand> kafka, Clock clock,
                                                        @Value("${messaging.result.followup.page-size:100}") int pageSize) {
        return new FollowupHttpDispatcher(db, mapper, kafka, clock, pageSize);
    }
}

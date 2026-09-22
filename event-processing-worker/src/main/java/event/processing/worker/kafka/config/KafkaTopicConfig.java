package event.processing.worker.kafka.config;

import event.common.topic.EventTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    private static final int PARTITION_COUNT = 3;
    private static final int REPLICA_COUNT = 1;

    @Bean
    public NewTopic eventRequestsTopic() {
        return TopicBuilder
                .name(EventTopics.EVENT_REQUESTS)
                .partitions(PARTITION_COUNT)
                .replicas(REPLICA_COUNT)
                .build();
    }

    @Bean
    public NewTopic eventRequestsDltTopic() {
        return TopicBuilder
                .name(EventTopics.EVENT_REQUESTS_DLT)
                .partitions(PARTITION_COUNT)
                .replicas(REPLICA_COUNT)
                .build();
    }
}
package event.delivery.ingress.kafka.config;

import event.common.delivery.DeliveryTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    private static final int PARTITION_COUNT = 3;
    private static final int REPLICA_COUNT = 1;

    @Bean
    public NewTopic deliveryRequestedTopic() {
        return TopicBuilder
                .name(DeliveryTopics.DELIVERY_REQUESTED)
                .partitions(PARTITION_COUNT)
                .replicas(REPLICA_COUNT)
                .build();
    }

    @Bean
    public NewTopic dispatchRequestedTopic() {
        return TopicBuilder
                .name(DeliveryTopics.DISPATCH_REQUESTED)
                .partitions(PARTITION_COUNT)
                .replicas(REPLICA_COUNT)
                .build();
    }

    @Bean
    public NewTopic deliveryRequestedDltTopic() {
        return TopicBuilder
                .name(DeliveryTopics.DELIVERY_REQUESTED_DLT)
                .partitions(PARTITION_COUNT)
                .replicas(REPLICA_COUNT)
                .build();
    }

    @Bean
    public NewTopic dispatchRequestedDltTopic() {
        return TopicBuilder
                .name(DeliveryTopics.DISPATCH_REQUESTED_DLT)
                .partitions(PARTITION_COUNT)
                .replicas(REPLICA_COUNT)
                .build();
    }
}

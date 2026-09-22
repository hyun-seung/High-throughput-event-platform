package event.api.event.ingress;

import event.api.event.ingress.dynamodb.EventIngressDynamoDbRepository;
import event.api.event.kafka.producer.EventProducer;
import event.common.message.EventMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;

import static event.api.event.ingress.config.EventIngressAsyncConfig.KAFKA_PUBLISH_RESULT_EXECUTOR;

@Slf4j
@Service
public class EventIngressService {

    private final EventIngressDynamoDbRepository dynamoDbRepository;
    private final EventProducer eventProducer;
    private final ExecutorService kafkaPublishResultExecutor;

    public EventIngressService(
            EventIngressDynamoDbRepository dynamoDbRepository,
            EventProducer eventProducer,
            @Qualifier(KAFKA_PUBLISH_RESULT_EXECUTOR)
            ExecutorService kafkaPublishResultExecutor
    ) {
        this.dynamoDbRepository = dynamoDbRepository;
        this.eventProducer = eventProducer;
        this.kafkaPublishResultExecutor = kafkaPublishResultExecutor;
    }

    public void publish(EventMessage message) {
        dynamoDbRepository.save(message);

        try {
            eventProducer.send(message)
                    .whenCompleteAsync(
                            (result, throwable) -> {
                                if (throwable == null) {
                                    markPublished(message.eventId());
                                    return;
                                }

                                markRetryRequired(message.eventId(), throwable);
                            },
                            kafkaPublishResultExecutor
                    );
        } catch (RuntimeException e) {
            markRetryRequired(message.eventId(), e);
        }
    }

    private void markPublished(String eventId) {
        try {
            dynamoDbRepository.markPublished(eventId);
        } catch (RuntimeException e) {
            log.error("Failed to update Kafka publish status to PUBLISHED. eventId={}", eventId, e);
        }
    }

    private void markRetryRequired(String eventId, Throwable publishException) {
        log.error("Failed to publish event to Kafka. eventId={}", eventId, publishException);

        try {
            dynamoDbRepository.markRetryRequired(eventId);
        } catch (RuntimeException e) {
            log.error("Failed to update Kafka publish status to RETRY_REQUIRED. eventId={}", eventId, e);
        }
    }
}

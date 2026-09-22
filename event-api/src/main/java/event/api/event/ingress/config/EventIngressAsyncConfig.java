package event.api.event.ingress.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class EventIngressAsyncConfig {

    public static final String KAFKA_PUBLISH_RESULT_EXECUTOR = "kafkaPublishResultExecutor";

    @Bean(name = KAFKA_PUBLISH_RESULT_EXECUTOR, destroyMethod = "close")
    public ExecutorService kafkaPublishResultExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}

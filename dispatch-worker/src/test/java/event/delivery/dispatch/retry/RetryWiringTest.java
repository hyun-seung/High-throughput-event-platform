package event.delivery.dispatch.retry;

import event.common.redis.DeliveryCache;
import event.delivery.dispatch.DispatchWorkerApplication;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.SpringApplication;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "KAFKA_TEST_BOOTSTRAP_SERVERS", matches = ".+")
@EnabledIfEnvironmentVariable(named = "DYNAMODB_TEST_ENDPOINT", matches = ".+")
class RetryWiringTest {
    @Test void applicationWiresRetryAndRealRedisWithoutStartingSharedConsumersOrLifecycle() {
        try (var app = SpringApplication.run(DispatchWorkerApplication.class,
                "--server.port=0", "--management.server.port=0", "--spring.kafka.listener.auto-startup=false",
                "--spring.kafka.admin.auto-create=false", "--dispatch.lifecycle.enabled=false",
                "--spring.kafka.bootstrap-servers=" + System.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS"),
                "--delivery.dynamodb.endpoint=" + System.getenv("DYNAMODB_TEST_ENDPOINT"),
                "--spring.data.redis.port=" + System.getenv().getOrDefault("REDIS_TEST_PORT", "16379"))) {
            assertNotNull(app.getBean(RetryPublisher.class)); assertNotNull(app.getBean(RetryConsumer.class));
            assertFalse(app.getBean(KafkaListenerEndpointRegistry.class).getListenerContainers().isEmpty());
            assertTrue(app.getBean(KafkaListenerEndpointRegistry.class).getListenerContainers().stream().noneMatch(c -> c.isRunning()));
            assertNull(app.getBean(DeliveryCache.class).completed("wiring-test-" + java.util.UUID.randomUUID()));
            assertNull(app.getBean(MeterRegistry.class).find("delivery.cache.errors").counter());
        }
    }
}

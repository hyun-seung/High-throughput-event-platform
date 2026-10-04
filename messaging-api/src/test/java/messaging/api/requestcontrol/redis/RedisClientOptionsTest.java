package messaging.api.requestcontrol.redis;

import messaging.common.redis.DeliveryCacheAutoConfiguration;
import io.lettuce.core.ClientOptions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import static org.junit.jupiter.api.Assertions.*;

class RedisClientOptionsTest {
    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DataRedisAutoConfiguration.class, DeliveryCacheAutoConfiguration.class))
            .withInitializer(app -> app.getBeanFactory().setConversionService(ApplicationConversionService.getSharedInstance()))
            .withPropertyValues("spring.data.redis.host=127.0.0.1", "spring.data.redis.port=1");

    @Test void bootWiresBoundedQueueAndRejectsCommandsOnlyWhileDisconnected() {
        context.run(app -> {
            assertNull(app.getStartupFailure());
            var factory = app.getBean(LettuceConnectionFactory.class);
            ClientOptions options = factory.getClientConfiguration().getClientOptions().orElseThrow();
            assertTrue(options.isAutoReconnect());
            assertEquals(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS, options.getDisconnectedBehavior());
            assertEquals(1024, options.getRequestQueueSize());
            var delay = factory.getClientConfiguration().getClientResources().orElseThrow().reconnectDelay();
            assertTrue(delay.createDelay(1).toMillis() >= 100);
            assertTrue(delay.createDelay(100).toSeconds() <= 30);
        });
    }

    @Test void invalidQueueLimitFailsAtStartup() {
        context.withPropertyValues("delivery.redis-recovery.request-queue-size=0")
                .run(app -> assertNotNull(app.getStartupFailure()));
    }
}

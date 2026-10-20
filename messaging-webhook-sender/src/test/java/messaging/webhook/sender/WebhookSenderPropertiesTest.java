package messaging.webhook.sender;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class WebhookSenderPropertiesTest {
    @Test
    void externalHttpRequiresExplicitLocalTestOptIn() {
        URI mock = URI.create("http://mock:18093/hook");
        assertThrows(IllegalArgumentException.class, () -> new WebhookSenderProperties.Destination(mock, false));
        assertEquals(mock, new WebhookSenderProperties.Destination(mock, true).url());
        assertEquals(URI.create("http://localhost:18093/hook"),
                new WebhookSenderProperties.Destination(URI.create("http://localhost:18093/hook"), false).url());
    }

    @Test
    void bindsDefaultAndConfiguredTurnBudgets() {
        var runner = new ApplicationContextRunner().withUserConfiguration(Settings.class)
                .withPropertyValues("messaging.webhook.sender.customers.42.url=https://example.org/hook");
        runner.run(context -> {
            assertNull(context.getStartupFailure());
            var settings = context.getBean(WebhookSenderProperties.class);
            assertEquals(4, settings.maxBatchesPerTurn());
            assertEquals(Duration.ofMillis(100), settings.maxTurnDuration());
        });
        runner.withPropertyValues("messaging.webhook.sender.max-batches-per-turn=2",
                "messaging.webhook.sender.max-turn-duration=20ms").run(context -> {
            assertNull(context.getStartupFailure());
            var settings = context.getBean(WebhookSenderProperties.class);
            assertEquals(2, settings.maxBatchesPerTurn());
            assertEquals(Duration.ofMillis(20), settings.maxTurnDuration());
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"max-batches-per-turn=0", "max-batches-per-turn=101", "max-turn-duration=0ms", "max-turn-duration=-1ms"})
    void rejectsDisabledOrUnboundedTurnBudgets(String property) {
        new ApplicationContextRunner().withUserConfiguration(Settings.class)
                .withPropertyValues("messaging.webhook.sender." + property)
                .run(context -> assertNotNull(context.getStartupFailure()));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(WebhookSenderProperties.class)
    static class Settings { }
}

package messaging.webhook.receive;

import messaging.common.dynamodb.DynamoDbAutoConfiguration;
import messaging.common.messages.RedisConnectionAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.util.ClassUtils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class OptionalInfrastructureConfigurationTest {
    @Test
    void kafkaOnlyApiStartsWithoutRedisOrDynamoDbLibraries() {
        var loader = getClass().getClassLoader();
        assertFalse(ClassUtils.isPresent("io.lettuce.core.resource.ClientResources", loader));
        assertFalse(ClassUtils.isPresent("software.amazon.awssdk.services.dynamodb.DynamoDbClient", loader));
        assertFalse(ClassUtils.isPresent("software.amazon.awssdk.http.apache5.Apache5HttpClient", loader));
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(DynamoDbAutoConfiguration.class,
                        RedisConnectionAutoConfiguration.class))
                .withPropertyValues("delivery.dynamodb.enabled=true")
                .run(context -> {
                    assertNull(context.getStartupFailure());
                    assertFalse(context.containsBean("dynamoDbClient"));
                    assertFalse(context.containsBean("messagingRedisSocketCustomizer"));
                });
    }
}

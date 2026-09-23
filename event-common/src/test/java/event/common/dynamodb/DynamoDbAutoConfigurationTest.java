package event.common.dynamodb;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DynamoDbAutoConfigurationTest {

    private Class<?> registeredConfiguration() throws ClassNotFoundException {
        String registered = ImportCandidates.load(AutoConfiguration.class, getClass().getClassLoader())
                .getCandidates().stream()
                .filter(name -> name.endsWith(".DynamoDbAutoConfiguration"))
                .findFirst().orElseThrow();
        return Class.forName(registered);
    }

    @Test
    void apiDoesNotCreateDynamoDbClientByDefault() throws Exception {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.register(registeredConfiguration());
            context.refresh();
            assertEquals(0, context.getBeansOfType(DynamoDbClient.class).size());
        }
    }

    @Test
    void workerCreatesLocalClientWhenEnabled() throws Exception {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("worker", Map.of(
                    "delivery.dynamodb.enabled", "true",
                    "delivery.dynamodb.endpoint", "http://localhost:8000")));
            context.register(registeredConfiguration());
            context.refresh();
            assertEquals(1, context.getBeansOfType(DynamoDbClient.class).size());
        }
    }
}

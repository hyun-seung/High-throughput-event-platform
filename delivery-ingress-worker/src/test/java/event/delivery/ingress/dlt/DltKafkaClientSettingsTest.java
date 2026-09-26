package event.delivery.ingress.dlt;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DltKafkaClientSettingsTest {
    @Test void springPropertiesReachProducerConsumerAndAdminWhileStandaloneToolsUseTheSameLimits() throws Exception {
        var environment = new StandardEnvironment();
        var yaml = new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml"));
        yaml.forEach(source -> environment.getPropertySources().addLast(source));
        var kafka = Binder.get(environment).bind("spring.kafka", KafkaProperties.class)
                .orElseThrow(() -> new IllegalStateException("Kafka application settings missing"));
        var standalone = new HashMap<String, Object>();
        standalone.put("delivery.timeout.ms", 10000);
        DltKafkaClientSettings.apply(standalone);

        for (var settings : new Map[]{kafka.buildProducerProperties(), kafka.buildConsumerProperties(),
                kafka.buildAdminProperties(), standalone}) {
            for (var key : new String[]{"reconnect.backoff.ms", "reconnect.backoff.max.ms",
                    "retry.backoff.ms", "retry.backoff.max.ms",
                    "socket.connection.setup.timeout.ms", "socket.connection.setup.timeout.max.ms"}) {
                assertEquals(Integer.parseInt(standalone.get(key).toString()), Integer.parseInt(settings.get(key).toString()), key);
            }
        }
        assertEquals("10000", kafka.buildProducerProperties().get("delivery.timeout.ms").toString());
        assertEquals(10000, standalone.get("delivery.timeout.ms"));
    }
}

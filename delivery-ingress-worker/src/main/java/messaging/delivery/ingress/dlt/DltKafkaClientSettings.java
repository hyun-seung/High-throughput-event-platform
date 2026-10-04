package messaging.delivery.ingress.dlt;

import java.util.Map;

/** The standalone DLT tools do not inherit Spring's spring.kafka.properties. */
final class DltKafkaClientSettings {
    private DltKafkaClientSettings() { }

    static void apply(Map<String, Object> properties) {
        properties.put("reconnect.backoff.ms", 500);
        properties.put("reconnect.backoff.max.ms", 10000);
        properties.put("retry.backoff.ms", 500);
        properties.put("retry.backoff.max.ms", 10000);
        properties.put("socket.connection.setup.timeout.ms", 5000);
        properties.put("socket.connection.setup.timeout.max.ms", 10000);
    }
}

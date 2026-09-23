package event.delivery.ingress.kafka.config;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class KafkaProducerConfigTest {
    @Test
    void configuresJsonWithoutTypeHeadersAndPreservesRawBytes() {
        var properties = new KafkaProperties();
        properties.getProducer().getProperties().put("spring.json.add.type.headers", "false");
        var factory = new KafkaProducerConfig().dltProducerFactory(properties);
        try (var serializer = factory.getValueSerializer()) {
            serializer.configure(properties.buildProducerProperties(), false);
            var headers = new RecordHeaders();
            byte[] json = serializer.serialize("delivery.dispatch-requested.v1", headers, Map.of("message", "test"));
            assertEquals("{\"message\":\"test\"}", new String(json, StandardCharsets.UTF_8));
            assertEquals(0, headers.toArray().length);
            byte[] raw = {0, 1, 2};
            assertArrayEquals(raw, serializer.serialize("delivery.dlt", raw));
        }
    }
}

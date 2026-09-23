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
        factory.reset();
    }

    @Test
    void preservesRawFailedKeysWhileSupportingNormalStringKeys() {
        var properties = new KafkaProperties();
        var factory = new KafkaProducerConfig().dltProducerFactory(properties);
        try (var serializer = factory.getKeySerializer()) {
            serializer.configure(properties.buildProducerProperties(), true);
            assertArrayEquals("delivery-1".getBytes(StandardCharsets.UTF_8),
                    serializer.serialize("delivery.dlt", "delivery-1"));
            byte[] raw = {(byte) 0xff, 0, 2};
            assertArrayEquals(raw, serializer.serialize("delivery.dlt", raw));
            assertNull(serializer.serialize("delivery.dlt", null));
        }
        factory.reset();
    }
}
